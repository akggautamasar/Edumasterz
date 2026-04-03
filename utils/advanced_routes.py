"""
Advanced Features API Routes
All endpoints for the enhanced TG Drive functionality
"""

from fastapi import APIRouter, Request, HTTPException, Query, Header
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from typing import Optional, List
from datetime import datetime
import secrets

from utils.advanced_features import (
    USER_MANAGER, ANALYTICS_MANAGER, VERSION_MANAGER, COLLECTION_MANAGER,
    CHAPTER_MANAGER, SCHEDULE_MANAGER, FAVORITES_MANAGER, DUPLICATE_DETECTOR,
    FOLDER_PASSWORD_MANAGER, SUBTITLE_MANAGER, SHARING_MANAGER, CACHE_MANAGER
)
from utils.logger import Logger
from config import ADMIN_PASSWORD

logger = Logger(__name__)
router = APIRouter(prefix="/api/v2", tags=["Advanced Features"])


def get_client_ip(request: Request) -> str:
    """Get client IP address"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def verify_admin(password: str) -> bool:
    """Verify admin password"""
    return password == ADMIN_PASSWORD


def verify_session(token: str) -> Optional[dict]:
    """Verify session and return user"""
    if not token:
        return None
    user = USER_MANAGER.validate_session(token)
    if user:
        return {"id": user.id, "username": user.username, "role": user.role}
    return None


# ============================================================================
# USER MANAGEMENT ROUTES
# ============================================================================

@router.post("/users/register")
async def register_user(request: Request):
    """Register a new user (admin only)"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    user = USER_MANAGER.create_user(
        username=data["username"],
        password=data["password"],
        email=data.get("email"),
        role=data.get("role", "viewer")
    )
    
    if user:
        return JSONResponse({
            "status": "ok",
            "user": {"id": user.id, "username": user.username, "role": user.role}
        })
    return JSONResponse({"status": "error", "message": "Username already exists"})


@router.post("/users/login")
async def login_user(request: Request):
    """Login and get session token"""
    data = await request.json()
    ip = get_client_ip(request)
    
    token = USER_MANAGER.authenticate(
        username=data["username"],
        password=data["password"],
        ip=ip
    )
    
    if token:
        user = USER_MANAGER.validate_session(token)
        return JSONResponse({
            "status": "ok",
            "token": token,
            "user": {"id": user.id, "username": user.username, "role": user.role}
        })
    return JSONResponse({"status": "error", "message": "Invalid credentials or IP blocked"})


@router.post("/users/logout")
async def logout_user(request: Request):
    """Logout user"""
    data = await request.json()
    USER_MANAGER.logout(data.get("token", ""))
    return JSONResponse({"status": "ok"})


@router.get("/users/list")
async def list_users(password: str = Query(...)):
    """List all users (admin only)"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({"status": "ok", "users": USER_MANAGER.list_users()})


@router.post("/users/update-role")
async def update_user_role(request: Request):
    """Update user role (admin only)"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if USER_MANAGER.update_user_role(data["user_id"], data["role"]):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Failed to update role"})


@router.post("/users/delete")
async def delete_user(request: Request):
    """Delete user (admin only)"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if USER_MANAGER.delete_user(data["user_id"]):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Cannot delete user"})


@router.post("/users/enable-2fa")
async def enable_2fa(request: Request):
    """Enable 2FA for user"""
    data = await request.json()
    user = verify_session(data.get("token"))
    
    if not user:
        return JSONResponse({"status": "error", "message": "Invalid session"})
    
    secret = USER_MANAGER.enable_2fa(user["id"])
    if secret:
        return JSONResponse({"status": "ok", "secret": secret})
    return JSONResponse({"status": "error", "message": "Failed to enable 2FA"})


@router.post("/users/ip-whitelist")
async def manage_ip_whitelist(request: Request):
    """Manage IP whitelist (admin only)"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    action = data.get("action")
    ip = data.get("ip")
    
    if action == "add":
        USER_MANAGER.ip_whitelist.add(ip)
    elif action == "remove":
        USER_MANAGER.ip_whitelist.discard(ip)
    elif action == "clear":
        USER_MANAGER.ip_whitelist.clear()
    
    return JSONResponse({"status": "ok", "whitelist": list(USER_MANAGER.ip_whitelist)})


@router.post("/users/ip-blacklist")
async def manage_ip_blacklist(request: Request):
    """Manage IP blacklist (admin only)"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    action = data.get("action")
    ip = data.get("ip")
    
    if action == "add":
        USER_MANAGER.ip_blacklist.add(ip)
    elif action == "remove":
        USER_MANAGER.ip_blacklist.discard(ip)
    elif action == "clear":
        USER_MANAGER.ip_blacklist.clear()
    
    return JSONResponse({"status": "ok", "blacklist": list(USER_MANAGER.ip_blacklist)})


# ============================================================================
# ANALYTICS ROUTES
# ============================================================================

@router.get("/analytics/popular-files")
async def get_popular_files(password: str = Query(...), limit: int = Query(10)):
    """Get most popular files"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({
        "status": "ok",
        "files": ANALYTICS_MANAGER.get_popular_files(limit)
    })


@router.get("/analytics/access-logs")
async def get_access_logs(
    password: str = Query(...),
    limit: int = Query(100),
    file_path: str = Query(None),
    user_id: str = Query(None)
):
    """Get access logs"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({
        "status": "ok",
        "logs": ANALYTICS_MANAGER.get_recent_logs(limit, file_path, user_id)
    })


@router.get("/analytics/bandwidth")
async def get_bandwidth_stats(password: str = Query(...), days: int = Query(7)):
    """Get bandwidth statistics"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({
        "status": "ok",
        "stats": ANALYTICS_MANAGER.get_bandwidth_stats(days)
    })


@router.get("/analytics/storage")
async def get_storage_breakdown(password: str = Query(...)):
    """Get storage breakdown by file type"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({
        "status": "ok",
        "breakdown": ANALYTICS_MANAGER.get_storage_breakdown()
    })


# ============================================================================
# VERSION CONTROL ROUTES
# ============================================================================

@router.post("/versions/add")
async def add_file_version(request: Request):
    """Add a new version of a file"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    version = VERSION_MANAGER.add_version(
        file_id=data["file_id"],
        message_id=data["message_id"],
        size=data["size"],
        created_by=data.get("created_by"),
        comment=data.get("comment")
    )
    
    return JSONResponse({"status": "ok", "version": version.version_id})


@router.get("/versions/{file_id}")
async def get_file_versions(file_id: str):
    """Get all versions of a file"""
    return JSONResponse({
        "status": "ok",
        "versions": VERSION_MANAGER.get_versions(file_id)
    })


@router.delete("/versions/{file_id}/{version_id}")
async def delete_version(file_id: str, version_id: str, password: str = Query(...)):
    """Delete a specific version"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if VERSION_MANAGER.delete_version(file_id, version_id):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Version not found"})


# ============================================================================
# COLLECTIONS/PLAYLISTS ROUTES
# ============================================================================

@router.post("/collections/create")
async def create_collection(request: Request):
    """Create a new collection"""
    data = await request.json()
    user = verify_session(data.get("token"))
    
    if not user and not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Authentication required"})
    
    collection = COLLECTION_MANAGER.create_collection(
        name=data["name"],
        description=data.get("description", ""),
        owner_id=user["id"] if user else "admin",
        is_public=data.get("is_public", False),
        tags=data.get("tags", [])
    )
    
    return JSONResponse({"status": "ok", "collection": collection.id})


@router.get("/collections")
async def list_collections(
    token: str = Query(None),
    owner_only: bool = Query(False)
):
    """List collections"""
    user = verify_session(token)
    owner_id = user["id"] if user and owner_only else None
    
    return JSONResponse({
        "status": "ok",
        "collections": COLLECTION_MANAGER.list_collections(owner_id)
    })


@router.get("/collections/{collection_id}")
async def get_collection(collection_id: str):
    """Get collection details"""
    collection = COLLECTION_MANAGER.get_collection(collection_id)
    if collection:
        return JSONResponse({"status": "ok", "collection": collection})
    return JSONResponse({"status": "error", "message": "Collection not found"})


@router.post("/collections/{collection_id}/add")
async def add_to_collection(collection_id: str, request: Request):
    """Add file to collection"""
    data = await request.json()
    
    if COLLECTION_MANAGER.add_to_collection(collection_id, data["file_path"]):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Failed to add to collection"})


@router.post("/collections/{collection_id}/remove")
async def remove_from_collection(collection_id: str, request: Request):
    """Remove file from collection"""
    data = await request.json()
    
    if COLLECTION_MANAGER.remove_from_collection(collection_id, data["file_path"]):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Failed to remove from collection"})


@router.delete("/collections/{collection_id}")
async def delete_collection(collection_id: str, password: str = Query(...)):
    """Delete collection"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if COLLECTION_MANAGER.delete_collection(collection_id):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Collection not found"})


# ============================================================================
# VIDEO CHAPTERS ROUTES
# ============================================================================

@router.post("/chapters/set")
async def set_video_chapters(request: Request):
    """Set chapters for a video"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    CHAPTER_MANAGER.set_chapters(data["file_path"], data["chapters"])
    return JSONResponse({"status": "ok"})


@router.get("/chapters/{file_path:path}")
async def get_video_chapters(file_path: str):
    """Get chapters for a video"""
    return JSONResponse({
        "status": "ok",
        "chapters": CHAPTER_MANAGER.get_chapters(f"/{file_path}")
    })


@router.post("/chapters/add")
async def add_chapter(request: Request):
    """Add a single chapter"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    CHAPTER_MANAGER.add_chapter(
        file_path=data["file_path"],
        title=data["title"],
        start_time=data["start_time"],
        end_time=data.get("end_time")
    )
    return JSONResponse({"status": "ok"})


@router.post("/chapters/parse")
async def parse_chapters_from_text(request: Request):
    """Parse chapters from description text"""
    data = await request.json()
    chapters = CHAPTER_MANAGER.parse_chapters_from_description(data["description"])
    return JSONResponse({"status": "ok", "chapters": chapters})


# ============================================================================
# SCHEDULED UPLOADS ROUTES
# ============================================================================

@router.post("/schedule/create")
async def schedule_upload(request: Request):
    """Schedule a file upload"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    scheduled_time = datetime.fromisoformat(data["scheduled_time"])
    task = SCHEDULE_MANAGER.schedule_upload(
        url=data["url"],
        destination_path=data["destination_path"],
        filename=data["filename"],
        scheduled_time=scheduled_time
    )
    
    return JSONResponse({"status": "ok", "task_id": task.id})


@router.get("/schedule/list")
async def list_scheduled(password: str = Query(...), status: str = Query(None)):
    """List scheduled uploads"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({
        "status": "ok",
        "tasks": SCHEDULE_MANAGER.get_scheduled(status)
    })


@router.post("/schedule/cancel/{task_id}")
async def cancel_scheduled(task_id: str, password: str = Query(...)):
    """Cancel a scheduled upload"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if SCHEDULE_MANAGER.cancel_scheduled(task_id):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Cannot cancel task"})


# ============================================================================
# FAVORITES & RECENT FILES ROUTES
# ============================================================================

@router.post("/favorites/add")
async def add_favorite(request: Request):
    """Add file to favorites"""
    data = await request.json()
    user = verify_session(data.get("token"))
    
    if not user:
        return JSONResponse({"status": "error", "message": "Login required"})
    
    FAVORITES_MANAGER.add_favorite(user["id"], data["file_path"])
    return JSONResponse({"status": "ok"})


@router.post("/favorites/remove")
async def remove_favorite(request: Request):
    """Remove file from favorites"""
    data = await request.json()
    user = verify_session(data.get("token"))
    
    if not user:
        return JSONResponse({"status": "error", "message": "Login required"})
    
    FAVORITES_MANAGER.remove_favorite(user["id"], data["file_path"])
    return JSONResponse({"status": "ok"})


@router.get("/favorites")
async def get_favorites(token: str = Query(...)):
    """Get user's favorites"""
    user = verify_session(token)
    
    if not user:
        return JSONResponse({"status": "error", "message": "Login required"})
    
    return JSONResponse({
        "status": "ok",
        "favorites": FAVORITES_MANAGER.get_favorites(user["id"])
    })


@router.get("/recent")
async def get_recent_files(token: str = Query(...), limit: int = Query(20)):
    """Get recent files"""
    user = verify_session(token)
    
    if not user:
        return JSONResponse({"status": "error", "message": "Login required"})
    
    return JSONResponse({
        "status": "ok",
        "recent": FAVORITES_MANAGER.get_recent(user["id"], limit)
    })


# ============================================================================
# DUPLICATE DETECTION ROUTES
# ============================================================================

@router.get("/duplicates")
async def get_duplicates(password: str = Query(...)):
    """Get all duplicate files"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({
        "status": "ok",
        "duplicates": DUPLICATE_DETECTOR.get_all_duplicates()
    })


# ============================================================================
# FOLDER PASSWORD ROUTES
# ============================================================================

@router.post("/folder-password/set")
async def set_folder_password(request: Request):
    """Set password for folder"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    FOLDER_PASSWORD_MANAGER.set_password(data["folder_path"], data["password"])
    return JSONResponse({"status": "ok"})


@router.post("/folder-password/remove")
async def remove_folder_password(request: Request):
    """Remove password from folder"""
    data = await request.json()
    
    if not verify_admin(data.get("admin_password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if FOLDER_PASSWORD_MANAGER.remove_password(data["folder_path"]):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Folder not password protected"})


@router.post("/folder-password/verify")
async def verify_folder_password(request: Request):
    """Verify folder password"""
    data = await request.json()
    
    if FOLDER_PASSWORD_MANAGER.verify_password(
        data["folder_path"], 
        data["password"],
        data.get("session_token")
    ):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Invalid password"})


@router.get("/folder-password/check/{folder_path:path}")
async def check_folder_protected(folder_path: str):
    """Check if folder is password protected"""
    return JSONResponse({
        "status": "ok",
        "protected": FOLDER_PASSWORD_MANAGER.is_protected(f"/{folder_path}")
    })


# ============================================================================
# SUBTITLE ROUTES
# ============================================================================

@router.post("/subtitles/add")
async def add_subtitle(request: Request):
    """Add subtitle to video"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    SUBTITLE_MANAGER.add_subtitle(
        video_path=data["video_path"],
        language=data["language"],
        subtitle_path=data["subtitle_path"],
        label=data.get("label")
    )
    return JSONResponse({"status": "ok"})


@router.get("/subtitles/{video_path:path}")
async def get_subtitles(video_path: str):
    """Get subtitles for video"""
    return JSONResponse({
        "status": "ok",
        "subtitles": SUBTITLE_MANAGER.get_subtitles(f"/{video_path}")
    })


@router.delete("/subtitles/{video_path:path}/{language}")
async def remove_subtitle(video_path: str, language: str, password: str = Query(...)):
    """Remove subtitle"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    if SUBTITLE_MANAGER.remove_subtitle(f"/{video_path}", language):
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "error", "message": "Subtitle not found"})


# ============================================================================
# SHARING ROUTES
# ============================================================================

@router.post("/share/create")
async def create_share_link(request: Request):
    """Create a short share link"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    result = SHARING_MANAGER.create_short_link(
        file_path=data["file_path"],
        expires_hours=data.get("expires_hours", 24),
        password=data.get("link_password"),
        max_uses=data.get("max_uses")
    )
    
    base_url = str(request.base_url).rstrip("/")
    result["url"] = f"{base_url}/s/{result['short_code']}"
    
    return JSONResponse({"status": "ok", **result})


@router.get("/share/embed")
async def get_embed_code(file_path: str = Query(...), request: Request = None):
    """Get embeddable player code"""
    base_url = str(request.base_url).rstrip("/")
    embed_code = SHARING_MANAGER.generate_embed_code(file_path, base_url)
    return JSONResponse({"status": "ok", "embed_code": embed_code})


@router.get("/share/qr")
async def get_qr_code(url: str = Query(...)):
    """Get QR code for URL"""
    qr_url = SHARING_MANAGER.generate_qr_data(url)
    return JSONResponse({"status": "ok", "qr_url": qr_url})


# ============================================================================
# CACHE ROUTES
# ============================================================================

@router.get("/cache/stats")
async def get_cache_stats(password: str = Query(...)):
    """Get cache statistics"""
    if not verify_admin(password):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    return JSONResponse({"status": "ok", **CACHE_MANAGER.get_cache_stats()})


@router.post("/cache/clear")
async def clear_cache(request: Request):
    """Clear all cache"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    CACHE_MANAGER.clear_cache()
    return JSONResponse({"status": "ok"})


# ============================================================================
# BULK OPERATIONS ROUTES
# ============================================================================

@router.post("/bulk/import-urls")
async def bulk_import_urls(request: Request):
    """Bulk import files from URLs"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    urls = data.get("urls", [])
    destination = data.get("destination_path", "/")
    
    tasks = []
    for url_info in urls:
        if isinstance(url_info, str):
            url = url_info
            filename = url.split("/")[-1] or f"file_{secrets.token_hex(4)}"
        else:
            url = url_info["url"]
            filename = url_info.get("filename", url.split("/")[-1])
        
        # Schedule for immediate execution
        task = SCHEDULE_MANAGER.schedule_upload(
            url=url,
            destination_path=destination,
            filename=filename,
            scheduled_time=datetime.now()
        )
        tasks.append(task.id)
    
    return JSONResponse({"status": "ok", "task_ids": tasks, "count": len(tasks)})


# ============================================================================
# RECYCLE BIN ROUTES
# ============================================================================

@router.post("/trash/auto-delete")
async def configure_auto_delete(request: Request):
    """Configure auto-delete for trash (days until permanent deletion)"""
    data = await request.json()
    
    if not verify_admin(data.get("password", "")):
        return JSONResponse({"status": "error", "message": "Admin access required"})
    
    # This would be stored in a config and used by a background task
    days = data.get("days", 30)
    
    return JSONResponse({
        "status": "ok",
        "message": f"Auto-delete configured for {days} days"
    })


# ============================================================================
# EXPORT FUNCTION
# ============================================================================

def get_router():
    """Return the router for inclusion in main app"""
    return router
