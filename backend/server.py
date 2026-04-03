"""
TG Drive Backend Server
Fixed video streaming with proper HTTP Range Request support
Added: Token-based access, tags/categories, search, and performance optimizations

This is an enhanced version for the Emergent preview environment.
When deployed to Render with valid Telegram credentials, the full functionality will work.
"""

import sys
import os

# Add parent directory to path to import existing modules
sys.path.insert(0, '/app')

import math
import mimetypes
import hashlib
import secrets
import time
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
from urllib.parse import quote, unquote

import aiofiles
from fastapi import FastAPI, HTTPException, Request, File, UploadFile, Form, Response, Query, Header
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from utils.logger import Logger

logger = Logger(__name__)

# Environment check - skip Telegram initialization if credentials are invalid
DEMO_MODE = True
try:
    from config import ADMIN_PASSWORD, MAX_FILE_SIZE, STORAGE_CHANNEL, API_ID, API_HASH
    if API_ID and API_HASH and str(API_ID) != "123456":
        DEMO_MODE = False
except Exception as e:
    logger.warning(f"Running in DEMO MODE - Telegram credentials not configured: {e}")
    ADMIN_PASSWORD = "admin"
    MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
    STORAGE_CHANNEL = -100123456789

# ============================================================================
# TOKEN MANAGEMENT FOR SECURE ACCESS
# ============================================================================

# Token storage: {token: {file_path, expires_at, password_protected, password_hash}}
ACCESS_TOKENS: Dict[str, Dict[str, Any]] = {}
TOKEN_EXPIRY_HOURS = 24  # Default token expiry

def generate_secure_token() -> str:
    """Generate a cryptographically secure token"""
    return secrets.token_urlsafe(32)

def hash_password(password: str) -> str:
    """Hash a password for secure storage"""
    return hashlib.sha256(password.encode()).hexdigest()

def create_access_token(
    file_path: str, 
    expiry_hours: int = TOKEN_EXPIRY_HOURS,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """Create a temporary access token for a file"""
    token = generate_secure_token()
    expires_at = datetime.now() + timedelta(hours=expiry_hours)
    
    token_data = {
        "file_path": file_path,
        "expires_at": expires_at,
        "password_protected": password is not None,
        "password_hash": hash_password(password) if password else None,
        "created_at": datetime.now(),
        "access_count": 0
    }
    
    ACCESS_TOKENS[token] = token_data
    logger.info(f"Created access token for {file_path}, expires in {expiry_hours} hours")
    return {"token": token, "expires_at": expires_at.isoformat()}

def validate_access_token(token: str, password: Optional[str] = None) -> Optional[str]:
    """Validate an access token and return the file path if valid"""
    if token not in ACCESS_TOKENS:
        logger.warning(f"Invalid token attempted: {token[:8]}...")
        return None
    
    token_data = ACCESS_TOKENS[token]
    
    # Check expiry
    if datetime.now() > token_data["expires_at"]:
        del ACCESS_TOKENS[token]
        logger.info(f"Token expired and removed: {token[:8]}...")
        return None
    
    # Check password if required
    if token_data["password_protected"]:
        if not password or hash_password(password) != token_data["password_hash"]:
            logger.warning(f"Invalid password for token: {token[:8]}...")
            return None
    
    # Increment access count
    token_data["access_count"] += 1
    
    return token_data["file_path"]

async def cleanup_expired_tokens():
    """Periodically clean up expired tokens"""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        now = datetime.now()
        expired = [t for t, d in ACCESS_TOKENS.items() if now > d["expires_at"]]
        for token in expired:
            del ACCESS_TOKENS[token]
        if expired:
            logger.info(f"Cleaned up {len(expired)} expired tokens")

# ============================================================================
# TAGS/CATEGORIES MANAGEMENT
# ============================================================================

# Tags storage: {file_id: [tags]}
FILE_TAGS: Dict[str, list] = {}

def add_tags_to_file(file_id: str, tags: list) -> None:
    """Add tags to a file"""
    if file_id not in FILE_TAGS:
        FILE_TAGS[file_id] = []
    FILE_TAGS[file_id].extend([t.lower().strip() for t in tags if t.strip()])
    FILE_TAGS[file_id] = list(set(FILE_TAGS[file_id]))  # Remove duplicates
    logger.info(f"Added tags to file {file_id}: {tags}")

def get_file_tags(file_id: str) -> list:
    """Get tags for a file"""
    return FILE_TAGS.get(file_id, [])

def search_by_tags(tags: list) -> list:
    """Search files by tags"""
    tags = [t.lower().strip() for t in tags]
    results = []
    for file_id, file_tags in FILE_TAGS.items():
        if any(tag in file_tags for tag in tags):
            results.append(file_id)
    return results

# ============================================================================
# IMPROVED STREAMING WITH HTTP RANGE SUPPORT
# ============================================================================

class_cache = {}

def parse_range_header(range_header: str, file_size: int) -> tuple:
    """
    Parse HTTP Range header and return (start, end) bytes.
    Handles various range formats:
    - bytes=0-499 (first 500 bytes)
    - bytes=500-999 (second 500 bytes)
    - bytes=-500 (last 500 bytes)
    - bytes=9500- (from byte 9500 to end)
    """
    if not range_header or not range_header.startswith('bytes='):
        return 0, file_size - 1
    
    range_spec = range_header[6:]  # Remove 'bytes='
    
    try:
        if range_spec.startswith('-'):
            # Last N bytes: bytes=-500
            suffix_length = int(range_spec[1:])
            start = max(0, file_size - suffix_length)
            end = file_size - 1
        elif range_spec.endswith('-'):
            # From offset to end: bytes=9500-
            start = int(range_spec[:-1])
            end = file_size - 1
        else:
            # Standard range: bytes=0-499
            parts = range_spec.split('-')
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else file_size - 1
        
        # Validate ranges
        start = max(0, min(start, file_size - 1))
        end = max(start, min(end, file_size - 1))
        
        logger.info(f"Parsed range: {range_header} -> {start}-{end}/{file_size}")
        return start, end
        
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse range header '{range_header}': {e}")
        return 0, file_size - 1

async def optimized_media_streamer(
    channel: int, 
    message_id: int, 
    file_name: str, 
    request: Request,
    chunk_size: int = 1024 * 1024  # 1MB chunks for optimal streaming
):
    """
    Optimized media streamer with full HTTP Range Request support.
    Features:
    - Proper 206 Partial Content responses
    - Optimized chunk sizes (1MB default)
    - Buffering optimization for fast start
    - Detailed logging for debugging
    - CORS support for browser playback
    """
    global class_cache
    
    range_header = request.headers.get("Range", "")
    
    logger.info(f"Stream request: file={file_name}, channel={channel}, msg_id={message_id}, range={range_header}")
    
    # Get client and setup streamer
    faster_client = get_client()
    
    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
    else:
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    
    try:
        # Get file properties
        file_id = await tg_connect.get_file_properties(channel, message_id)
        file_size = file_id.file_size
        
        logger.info(f"File properties: size={file_size}, mime={file_id.mime_type}")
        
        if file_size == 0:
            logger.error(f"File size is 0 for {file_name}")
            raise HTTPException(status_code=404, detail="File not found or empty")
        
        # Parse range request
        from_bytes, until_bytes = parse_range_header(range_header, file_size)
        
        # Validate range
        if from_bytes >= file_size or until_bytes >= file_size or from_bytes > until_bytes:
            logger.warning(f"Invalid range: {from_bytes}-{until_bytes} for file size {file_size}")
            return Response(
                status_code=416,
                content="Range Not Satisfiable",
                headers={
                    "Content-Range": f"bytes */{file_size}",
                    "Accept-Ranges": "bytes"
                }
            )
        
        # Ensure until_bytes doesn't exceed file size
        until_bytes = min(until_bytes, file_size - 1)
        
        # Calculate streaming parameters
        offset = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset
        last_part_cut = (until_bytes % chunk_size) + 1
        
        req_length = until_bytes - from_bytes + 1
        part_count = math.ceil((until_bytes + 1) / chunk_size) - math.floor(offset / chunk_size)
        
        logger.info(f"Streaming: offset={offset}, parts={part_count}, req_length={req_length}")
        
        # Generate the streaming body
        async def generate_chunks():
            """Async generator that yields file chunks"""
            try:
                async for chunk in tg_connect.yield_file(
                    file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size
                ):
                    yield chunk
            except Exception as e:
                logger.error(f"Error during streaming: {e}")
                raise
        
        # Determine content type and disposition
        mime_type = mimetypes.guess_type(file_name.lower())[0] or "application/octet-stream"
        
        # Force video MIME types for common video extensions
        video_extensions = {'.mp4': 'video/mp4', '.mkv': 'video/x-matroska', '.webm': 'video/webm', 
                          '.avi': 'video/x-msvideo', '.mov': 'video/quicktime', '.m4v': 'video/mp4'}
        ext = Path(file_name).suffix.lower()
        if ext in video_extensions:
            mime_type = video_extensions[ext]
        
        # Use inline for streamable content
        disposition = "inline" if any(x in mime_type for x in ["video/", "audio/", "image/", "/html"]) else "attachment"
        
        # Determine if this is a range request
        is_range_request = bool(range_header)
        status_code = 206 if is_range_request else 200
        
        # Build response headers
        headers = {
            "Content-Type": mime_type,
            "Content-Length": str(req_length),
            "Accept-Ranges": "bytes",
            "Content-Disposition": f'{disposition}; filename="{quote(file_name)}"',
            # CORS headers for browser playback
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
            # Caching headers
            "Cache-Control": "public, max-age=3600",
            # Performance hints
            "X-Content-Type-Options": "nosniff",
        }
        
        # Add Content-Range header for range requests
        if is_range_request:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
        
        logger.info(f"Response: status={status_code}, Content-Range=bytes {from_bytes}-{until_bytes}/{file_size}")
        
        return StreamingResponse(
            content=generate_chunks(),
            status_code=status_code,
            headers=headers,
            media_type=mime_type
        )
        
    except Exception as e:
        logger.error(f"Stream error for {file_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Streaming error: {str(e)}")

# ============================================================================
# FASTAPI APPLICATION SETUP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global DEMO_MODE
    
    if DEMO_MODE:
        logger.info("Starting in DEMO MODE - Telegram features disabled")
        logger.info("Configure valid Telegram credentials to enable full functionality")
    else:
        try:
            from utils.extra import reset_cache_dir, auto_ping_website
            from utils.clients import initialize_clients
            
            # Reset cache directory
            reset_cache_dir()
            
            # Initialize Telegram clients
            await initialize_clients()
            
            # Start background tasks
            asyncio.create_task(auto_ping_website())
        except Exception as e:
            logger.error(f"Failed to initialize Telegram clients: {e}")
            DEMO_MODE = True
    
    # Start token cleanup task
    asyncio.create_task(cleanup_expired_tokens())
    
    logger.info("TG Drive server started with enhanced streaming")
    yield
    logger.info("TG Drive server shutting down")

app = FastAPI(
    title="TG Drive - Enhanced Streaming",
    description="Telegram-based file storage with YouTube-like video streaming",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware for browser compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Length", "Content-Range", "Accept-Ranges"]
)

# ============================================================================
# STATIC FILE AND PAGE ROUTES
# ============================================================================

@app.get("/")
async def home_page():
    """Serve home page"""
    return FileResponse("/app/website/home.html")

@app.get("/stream")
async def stream_page():
    """Serve video player page"""
    return FileResponse("/app/website/VideoPlayer.html")

@app.get("/fast-player")
async def fast_player_page():
    """Serve fast player page"""
    return FileResponse("/app/website/FastPlayer.html")

@app.get("/pdf-viewer")
async def pdf_viewer_page():
    """Serve PDF viewer page"""
    return FileResponse("/app/website/PDFViewer.html")

@app.get("/static/{file_path:path}")
async def static_files(file_path: str):
    """Serve static files"""
    if "apiHandler.js" in file_path:
        with open(Path("/app/website/static/js/apiHandler.js")) as f:
            content = f.read()
            content = content.replace("MAX_FILE_SIZE__SDGJDG", str(MAX_FILE_SIZE))
        return Response(content=content, media_type="application/javascript")
    return FileResponse(f"/app/website/static/{file_path}")

# ============================================================================
# FILE STREAMING ROUTES (ENHANCED)
# ============================================================================

@app.options("/file")
async def file_options():
    """Handle CORS preflight for file endpoint"""
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type, Authorization",
            "Access-Control-Max-Age": "86400"
        }
    )

@app.head("/file")
async def file_head(request: Request):
    """Handle HEAD requests for file metadata"""
    from utils.directoryHandler import DRIVE_DATA
    
    path = request.query_params.get("path")
    if not path:
        raise HTTPException(status_code=400, detail="Path parameter required")
    
    try:
        file = DRIVE_DATA.get_file(path)
        mime_type = mimetypes.guess_type(file.name.lower())[0] or "application/octet-stream"
        
        return Response(
            status_code=200,
            headers={
                "Content-Type": mime_type,
                "Content-Length": str(file.size),
                "Accept-Ranges": "bytes",
                "Content-Disposition": f'inline; filename="{quote(file.name)}"'
            }
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")

@app.get("/file")
async def dl_file(request: Request):
    """
    Enhanced file download/stream endpoint with proper HTTP Range support.
    Supports:
    - Range requests for seeking (206 Partial Content)
    - Quality selection for encoded videos
    - Token-based access
    """
    from utils.directoryHandler import DRIVE_DATA

    path = request.query_params.get("path")
    quality = request.query_params.get("quality", "original")
    token = request.query_params.get("token")
    password = request.query_params.get("password")
    
    logger.info(f"File request: path={path}, quality={quality}, token={'yes' if token else 'no'}")
    
    if not path:
        raise HTTPException(status_code=400, detail="Path parameter required")
    
    # Validate token if provided
    if token:
        validated_path = validate_access_token(token, password)
        if not validated_path:
            raise HTTPException(status_code=403, detail="Invalid or expired token")
        if validated_path != path:
            raise HTTPException(status_code=403, detail="Token does not match requested file")
    
    try:
        file = DRIVE_DATA.get_file(path)
    except Exception as e:
        logger.error(f"File not found: {path} - {e}")
        raise HTTPException(status_code=404, detail="File not found")
    
    # Check for encoded quality versions
    if quality != "original" and hasattr(file, 'encoded_versions') and file.encoded_versions:
        if quality in file.encoded_versions:
            encoded_version = file.encoded_versions[quality]
            return await optimized_media_streamer(
                STORAGE_CHANNEL, 
                encoded_version['message_id'], 
                f"{file.name}_{quality}", 
                request
            )
    
    # Determine channel for streaming
    if hasattr(file, 'is_fast_import') and file.is_fast_import and file.source_channel:
        channel = file.source_channel
        logger.info(f"Using fast import source channel {channel}")
    else:
        channel = STORAGE_CHANNEL
    
    return await optimized_media_streamer(channel, file.file_id, file.name, request)

@app.get("/secure/{token}")
async def secure_file_access(
    token: str, 
    request: Request,
    password: Optional[str] = Query(None)
):
    """
    Access a file using a secure temporary token.
    URL format: /secure/{token}?password=optional_password
    """
    file_path = validate_access_token(token, password)
    if not file_path:
        raise HTTPException(status_code=403, detail="Invalid, expired, or password-protected token")
    
    from utils.directoryHandler import DRIVE_DATA
    
    try:
        file = DRIVE_DATA.get_file(file_path)
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Determine channel
    if hasattr(file, 'is_fast_import') and file.is_fast_import and file.source_channel:
        channel = file.source_channel
    else:
        channel = STORAGE_CHANNEL
    
    return await optimized_media_streamer(channel, file.file_id, file.name, request)

# ============================================================================
# TOKEN MANAGEMENT API
# ============================================================================

@app.post("/api/createShareToken")
async def create_share_token(request: Request):
    """Create a temporary share token for a file"""
    data = await request.json()
    
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    file_path = data.get("path")
    expiry_hours = data.get("expiry_hours", TOKEN_EXPIRY_HOURS)
    file_password = data.get("file_password")  # Optional password protection
    
    if not file_path:
        return JSONResponse({"status": "Path required"})
    
    # Verify file exists
    from utils.directoryHandler import DRIVE_DATA
    try:
        DRIVE_DATA.get_file(file_path)
    except Exception:
        return JSONResponse({"status": "File not found"})
    
    token_info = create_access_token(file_path, expiry_hours, file_password)
    
    # Generate full URL
    base_url = str(request.base_url).rstrip('/')
    share_url = f"{base_url}/secure/{token_info['token']}"
    if file_password:
        share_url += "?password=YOUR_PASSWORD"
    
    return JSONResponse({
        "status": "ok",
        "token": token_info["token"],
        "expires_at": token_info["expires_at"],
        "share_url": share_url,
        "password_protected": file_password is not None
    })

@app.post("/api/revokeShareToken")
async def revoke_share_token(request: Request):
    """Revoke a share token"""
    data = await request.json()
    
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    token = data.get("token")
    if token in ACCESS_TOKENS:
        del ACCESS_TOKENS[token]
        return JSONResponse({"status": "ok", "message": "Token revoked"})
    
    return JSONResponse({"status": "Token not found"})

# ============================================================================
# TAGS/CATEGORIES API
# ============================================================================

@app.post("/api/addTags")
async def add_tags(request: Request):
    """Add tags to a file"""
    data = await request.json()
    
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    file_path = data.get("path")
    tags = data.get("tags", [])
    
    if not file_path:
        return JSONResponse({"status": "Path required"})
    
    # Get file ID from path
    from utils.directoryHandler import DRIVE_DATA
    
    # Handle DEMO MODE where DRIVE_DATA is None
    if DRIVE_DATA is None:
        return JSONResponse({"status": "File not found", "note": "Running in DEMO MODE"})
    
    try:
        file = DRIVE_DATA.get_file(file_path)
        add_tags_to_file(file.id, tags)
        return JSONResponse({"status": "ok", "tags": get_file_tags(file.id)})
    except Exception as e:
        return JSONResponse({"status": str(e)})

@app.post("/api/getTags")
async def get_tags(request: Request):
    """Get tags for a file"""
    data = await request.json()
    file_path = data.get("path")
    
    if not file_path:
        return JSONResponse({"status": "Path required"})
    
    from utils.directoryHandler import DRIVE_DATA
    try:
        file = DRIVE_DATA.get_file(file_path)
        return JSONResponse({"status": "ok", "tags": get_file_tags(file.id)})
    except Exception:
        return JSONResponse({"status": "File not found"})

@app.post("/api/searchByTags")
async def search_tags(request: Request):
    """Search files by tags"""
    data = await request.json()
    tags = data.get("tags", [])
    
    if not tags:
        return JSONResponse({"status": "Tags required"})
    
    file_ids = search_by_tags(tags)
    return JSONResponse({"status": "ok", "file_ids": file_ids, "count": len(file_ids)})

@app.get("/api/allTags")
async def get_all_tags():
    """Get all unique tags in the system"""
    all_tags = set()
    for tags in FILE_TAGS.values():
        all_tags.update(tags)
    return JSONResponse({"status": "ok", "tags": sorted(list(all_tags))})

# ============================================================================
# ENHANCED SEARCH API
# ============================================================================

@app.post("/api/search")
async def enhanced_search(request: Request):
    """
    Enhanced search endpoint supporting:
    - Filename search
    - Tag search
    - Combined search
    """
    from utils.directoryHandler import DRIVE_DATA
    
    # Handle DEMO MODE where DRIVE_DATA is None
    if DRIVE_DATA is None:
        return JSONResponse({
            "status": "ok",
            "results": [],
            "count": 0,
            "note": "Running in DEMO MODE - no files available"
        })
    
    data = await request.json()
    query = data.get("query", "").strip()
    tags = data.get("tags", [])
    file_type = data.get("type")  # Optional: video, audio, document, image
    
    results = []
    
    # Search by filename
    if query:
        name_results = DRIVE_DATA.search_file_folder(query)
        for file_id, file_obj in name_results.items():
            if file_obj.type == "file":
                results.append({
                    "id": file_obj.id,
                    "name": file_obj.name,
                    "size": file_obj.size,
                    "path": file_obj.path,
                    "tags": get_file_tags(file_obj.id)
                })
    
    # Filter by tags
    if tags:
        tag_file_ids = search_by_tags(tags)
        if query:
            # Intersection: files matching both name and tags
            results = [r for r in results if r["id"] in tag_file_ids]
        else:
            # Just tag search
            for file_id in tag_file_ids:
                # Get file details (simplified)
                results.append({
                    "id": file_id,
                    "tags": get_file_tags(file_id)
                })
    
    # Filter by type if specified
    if file_type:
        type_extensions = {
            "video": [".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"],
            "audio": [".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a"],
            "document": [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt"],
            "image": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"]
        }
        if file_type in type_extensions:
            exts = type_extensions[file_type]
            results = [r for r in results if any(r.get("name", "").lower().endswith(ext) for ext in exts)]
    
    return JSONResponse({
        "status": "ok",
        "results": results,
        "count": len(results)
    })

# ============================================================================
# IMPORT ALL EXISTING API ROUTES FROM MAIN.PY
# ============================================================================

# Password check
@app.post("/api/checkPassword")
async def check_password(request: Request):
    data = await request.json()
    if data["pass"] == ADMIN_PASSWORD:
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "Invalid password"})

# Directory operations
@app.post("/api/createNewFolder")
async def api_new_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    logger.info(f"createNewFolder {data}")
    folder_data = DRIVE_DATA.get_directory(data["path"]).contents
    for id in folder_data:
        f = folder_data[id]
        if f.type == "folder":
            if f.name == data["name"]:
                return JSONResponse({"status": "Folder with the name already exist in current directory"})
    
    DRIVE_DATA.new_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})

@app.post("/api/getDirectory")
async def api_get_directory(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    import urllib.parse
    
    data = await request.json()
    
    if data["password"] == ADMIN_PASSWORD:
        is_admin = True
    else:
        is_admin = False
    
    auth = data.get("auth")
    sort_by = data.get("sort_by", "date")
    sort_order = data.get("sort_order", "desc")
    
    logger.info(f"getFolder {data}")
    
    if data["path"] == "/trash":
        data_result = {"contents": DRIVE_DATA.get_trashed_files_folders()}
        folder_data = convert_class_to_dict(data_result, isObject=False, showtrash=True, sort_by=sort_by, sort_order=sort_order)
    elif "/search_" in data["path"]:
        query = urllib.parse.unquote(data["path"].split("_", 1)[1])
        data_result = {"contents": DRIVE_DATA.search_file_folder(query)}
        folder_data = convert_class_to_dict(data_result, isObject=False, showtrash=False, sort_by=sort_by, sort_order=sort_order)
    elif "/share_" in data["path"]:
        path = data["path"].split("_", 1)[1]
        folder_data, auth_home_path = DRIVE_DATA.get_directory(path, is_admin, auth)
        auth_home_path = auth_home_path.replace("//", "/") if auth_home_path else None
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False, sort_by=sort_by, sort_order=sort_order)
        return JSONResponse({"status": "ok", "data": folder_data, "auth_home_path": auth_home_path})
    else:
        folder_data = DRIVE_DATA.get_directory(data["path"])
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False, sort_by=sort_by, sort_order=sort_order)
    
    return JSONResponse({"status": "ok", "data": folder_data, "auth_home_path": None})

# Upload handling
SAVE_PROGRESS = {}

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    path: str = Form(...),
    password: str = Form(...),
    id: str = Form(...),
    total_size: str = Form(...)
):
    from utils.uploader import start_file_uploader
    global SAVE_PROGRESS
    
    if password != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    total_size = int(total_size)
    SAVE_PROGRESS[id] = ("running", 0, total_size)
    
    ext = file.filename.lower().split(".")[-1]
    cache_dir = Path("/app/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    file_location = cache_dir / f"{id}.{ext}"
    
    file_size = 0
    
    async with aiofiles.open(file_location, "wb") as buffer:
        while chunk := await file.read(1024 * 1024):
            SAVE_PROGRESS[id] = ("running", file_size, total_size)
            file_size += len(chunk)
            if file_size > MAX_FILE_SIZE:
                await buffer.close()
                file_location.unlink()
                raise HTTPException(status_code=400, detail=f"File size exceeds {MAX_FILE_SIZE} bytes limit")
            await buffer.write(chunk)
    
    SAVE_PROGRESS[id] = ("completed", file_size, file_size)
    asyncio.create_task(start_file_uploader(file_location, id, path, file.filename, file_size))
    
    return JSONResponse({"id": id, "status": "ok"})

@app.post("/api/getSaveProgress")
async def get_save_progress(request: Request):
    global SAVE_PROGRESS
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        progress = SAVE_PROGRESS[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})

@app.post("/api/getUploadProgress")
async def get_upload_progress(request: Request):
    from utils.uploader import PROGRESS_CACHE
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        progress = PROGRESS_CACHE[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})

@app.post("/api/cancelUpload")
async def cancel_upload(request: Request):
    from utils.uploader import STOP_TRANSMISSION
    from utils.downloader import STOP_DOWNLOAD
    
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    STOP_TRANSMISSION.append(data["id"])
    STOP_DOWNLOAD.append(data["id"])
    return JSONResponse({"status": "ok"})

# File/Folder operations
@app.post("/api/renameFileFolder")
async def rename_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    DRIVE_DATA.rename_file_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})

@app.post("/api/trashFileFolder")
async def trash_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    DRIVE_DATA.trash_file_folder(data["path"], data["trash"])
    return JSONResponse({"status": "ok"})

@app.post("/api/deleteFileFolder")
async def delete_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    DRIVE_DATA.delete_file_folder(data["path"])
    return JSONResponse({"status": "ok"})

@app.post("/api/moveFileFolder")
async def move_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        DRIVE_DATA.move_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})

@app.post("/api/copyFileFolder")
async def copy_file_folder(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        DRIVE_DATA.copy_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})

@app.post("/api/getFolderTree")
async def get_folder_tree(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        folder_tree = DRIVE_DATA.get_folder_tree()
        return JSONResponse({"status": "ok", "data": folder_tree})
    except Exception as e:
        return JSONResponse({"status": str(e)})

@app.post("/api/getFolderShareAuth")
async def getFolderShareAuth(request: Request):
    from utils.directoryHandler import DRIVE_DATA
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        auth = DRIVE_DATA.get_folder_auth(data["path"])
        return JSONResponse({"status": "ok", "auth": auth})
    except:
        return JSONResponse({"status": "not found"})

# Download from URL
@app.post("/api/getFileInfoFromUrl")
async def getFileInfoFromUrl(request: Request):
    from utils.downloader import get_file_info_from_url
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        file_info = await get_file_info_from_url(data["url"])
        return JSONResponse({"status": "ok", "data": file_info})
    except Exception as e:
        return JSONResponse({"status": str(e)})

@app.post("/api/startFileDownloadFromUrl")
async def startFileDownloadFromUrl(request: Request):
    from utils.downloader import download_file
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        id = getRandomID()
        asyncio.create_task(download_file(data["url"], id, data["path"], data["filename"], data["singleThreaded"]))
        return JSONResponse({"status": "ok", "id": id})
    except Exception as e:
        return JSONResponse({"status": str(e)})

@app.post("/api/getFileDownloadProgress")
async def getFileDownloadProgress(request: Request):
    from utils.downloader import DOWNLOAD_PROGRESS
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        progress = DOWNLOAD_PROGRESS[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})

# Smart Import
@app.post("/api/smartBulkImport")
async def smart_bulk_import(request: Request):
    from utils.fast_import import SMART_IMPORT_MANAGER
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        client = get_client()
        channel_identifier = data["channel"]
        destination_folder = data["path"]
        start_msg_id = data.get("start_msg_id")
        end_msg_id = data.get("end_msg_id")
        import_mode = data.get("import_mode", "auto")
        
        imported_count, total_files, used_fast_import = await SMART_IMPORT_MANAGER.smart_bulk_import(
            client, channel_identifier, destination_folder, start_msg_id, end_msg_id, import_mode
        )
        
        return JSONResponse({
            "status": "ok",
            "imported": imported_count,
            "total": total_files,
            "method": "fast_import" if used_fast_import else "regular_import"
        })
    except Exception as e:
        logger.error(f"Smart bulk import error: {e}")
        return JSONResponse({"status": str(e)})

@app.post("/api/checkChannelAdmin")
async def check_channel_admin(request: Request):
    from utils.fast_import import SMART_IMPORT_MANAGER
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        client = get_client()
        channel_identifier = data["channel"]
        is_valid, result, is_admin = await SMART_IMPORT_MANAGER.validate_channel_access(client, channel_identifier)
        
        if not is_valid:
            return JSONResponse({"status": "error", "message": result})
        
        return JSONResponse({
            "status": "ok",
            "is_admin": is_admin,
            "channel_name": result.title or result.username or str(result.id)
        })
    except Exception as e:
        logger.error(f"Check channel admin error: {e}")
        return JSONResponse({"status": "error", "message": str(e)})

# Video Encoding
@app.post("/api/encodeVideo")
async def encode_video(request: Request):
    from utils.video_encoder import VIDEO_ENCODER
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        file_path = data["file_path"]
        qualities = data["qualities"]
        encoding_id = getRandomID()
        
        asyncio.create_task(VIDEO_ENCODER.encode_video_manual(file_path, qualities, encoding_id))
        return JSONResponse({"status": "ok", "encoding_id": encoding_id})
    except Exception as e:
        logger.error(f"Error starting video encoding: {e}")
        return JSONResponse({"status": str(e)})

@app.post("/api/getEncodingProgress")
async def get_encoding_progress(request: Request):
    from utils.video_encoder import VIDEO_ENCODER
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        encoding_id = data["encoding_id"]
        progress = VIDEO_ENCODER.get_encoding_progress(encoding_id)
        return JSONResponse({"status": "ok", "data": progress})
    except Exception as e:
        logger.error(f"Error getting encoding progress: {e}")
        return JSONResponse({"status": str(e)})

@app.post("/api/checkVideoEncodingSupport")
async def check_video_encoding_support(request: Request):
    from utils.video_encoder import VIDEO_ENCODER
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        ffmpeg_available = VIDEO_ENCODER.check_ffmpeg()
        return JSONResponse({
            "status": "ok",
            "ffmpeg_available": ffmpeg_available,
            "supported_qualities": list(VIDEO_ENCODER.resolutions.keys()) if ffmpeg_available else []
        })
    except Exception as e:
        logger.error(f"Error checking encoding support: {e}")
        return JSONResponse({"status": str(e)})

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse({
        "status": "ok",
        "version": "2.0.0",
        "features": ["streaming", "range-requests", "tokens", "tags", "search"]
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
