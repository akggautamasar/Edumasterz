"""
Enhanced ByteStreamer for Telegram file streaming.
Fixed for Pyrogram version compatibility (KurimuzonAkuma fork).
Optimized for YouTube-like video streaming with proper chunking.
"""

import asyncio
from typing import Dict, Union
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from utils.logger import Logger

logger = Logger(__name__)

# Telegram DC addresses for auth key creation
DC_ADDRESSES = {
    1: ("149.154.175.53", 443),
    2: ("149.154.167.51", 443),
    3: ("149.154.175.100", 443),
    4: ("149.154.167.91", 443),
    5: ("91.108.56.130", 443),
}

DC_ADDRESSES_TEST = {
    1: ("149.154.175.10", 443),
    2: ("149.154.167.40", 443),
    3: ("149.154.175.117", 443),
}


class ByteStreamer:
    """
    Handles streaming of files from Telegram with optimized chunking.
    Supports range requests for video seeking.
    """
    
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60  # 30 minutes cache cleanup
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, channel, message_id: int) -> FileId:
        """Get file properties, using cache if available."""
        if message_id not in self.cached_file_ids:
            await self.generate_file_properties(channel, message_id)
        return self.cached_file_ids[message_id]

    async def generate_file_properties(self, channel, message_id: int) -> FileId:
        """Generate and cache file properties from Telegram."""
        file_id = await get_file_ids(self.client, channel, message_id)
        if not file_id:
            raise Exception("FileNotFound")
        self.cached_file_ids[message_id] = file_id
        return self.cached_file_ids[message_id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        This is required for getting the bytes from Telegram servers.
        
        Fixed for KurimuzonAkuma Pyrogram fork compatibility.
        """
        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
            test_mode = await client.storage.test_mode()
            
            if file_id.dc_id != await client.storage.dc_id():
                # Need to create auth for different DC
                # Get DC address for the target DC
                dc_addresses = DC_ADDRESSES_TEST if test_mode else DC_ADDRESSES
                
                if file_id.dc_id in dc_addresses:
                    server_address, port = dc_addresses[file_id.dc_id]
                else:
                    # Fallback to default address pattern
                    server_address = f"149.154.167.{40 + file_id.dc_id}"
                    port = 443
                
                logger.debug(f"Creating auth for DC {file_id.dc_id} at {server_address}:{port}")
                
                try:
                    # New Auth signature: Auth(client, dc_id, server_address, port, test_mode)
                    auth = Auth(
                        client,
                        file_id.dc_id,
                        server_address,
                        port,
                        test_mode
                    )
                    auth_key = await auth.create()
                except Exception as e:
                    logger.error(f"Failed to create auth for DC {file_id.dc_id}: {e}")
                    raise
                
                media_session = Session(
                    client,
                    file_id.dc_id,
                    auth_key,
                    test_mode,
                    is_media=True,
                )
                await media_session.start()

                # Export and import authorization
                for attempt in range(6):
                    try:
                        exported_auth = await client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                        )

                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        logger.debug(f"Successfully imported auth for DC {file_id.dc_id}")
                        break
                    except AuthBytesInvalid:
                        logger.debug(f"Invalid authorization bytes for DC {file_id.dc_id}, attempt {attempt + 1}")
                        if attempt == 5:
                            await media_session.stop()
                            raise AuthBytesInvalid
                        continue
                    except Exception as e:
                        logger.error(f"Auth import error for DC {file_id.dc_id}: {e}")
                        if attempt == 5:
                            await media_session.stop()
                            raise
            else:
                # Same DC, use existing auth key
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    test_mode,
                    is_media=True,
                )
                await media_session.start()
                
            logger.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            logger.debug(f"Using cached media session for DC {file_id.dc_id}")
            
        return media_session

    @staticmethod
    async def get_location(
        file_id: FileId,
    ) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        """
        Returns the file location for the media file.
        Handles different file types (photos, documents, chat photos).
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """
        Async generator that yields the bytes of the media file.
        
        Optimized for video streaming with:
        - Configurable chunk sizes (default 1MB for optimal streaming)
        - Proper byte slicing for range requests
        - Error handling for network issues
        
        Args:
            file_id: The Telegram file ID object
            offset: Starting byte offset (aligned to chunk_size)
            first_part_cut: Bytes to skip in first chunk
            last_part_cut: Bytes to include in last chunk
            part_count: Total number of chunks to stream
            chunk_size: Size of each chunk in bytes
        """
        client = self.client
        logger.debug(f"Starting file stream: offset={offset}, parts={part_count}, chunk_size={chunk_size}")
        
        try:
            media_session = await self.generate_media_session(client, file_id)
        except Exception as e:
            logger.error(f"Failed to generate media session: {e}")
            raise

        current_part = 1
        location = await self.get_location(file_id)

        try:
            r = await media_session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=chunk_size
                ),
            )
            
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        logger.debug(f"Empty chunk received at part {current_part}")
                        break
                    
                    # Apply byte slicing based on position
                    if part_count == 1:
                        # Single chunk: slice both start and end
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        # First chunk: slice start only
                        yield chunk[first_part_cut:]
                    elif current_part == part_count:
                        # Last chunk: slice end only
                        yield chunk[:last_part_cut]
                    else:
                        # Middle chunk: yield full chunk
                        yield chunk

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        break

                    # Fetch next chunk
                    r = await media_session.invoke(
                        raw.functions.upload.GetFile(
                            location=location, offset=offset, limit=chunk_size
                        ),
                    )
            else:
                logger.warning(f"Unexpected response type: {type(r)}")
                
        except (TimeoutError, AttributeError) as e:
            logger.warning(f"Stream interrupted: {e}")
        except Exception as e:
            logger.error(f"Stream error: {e}")
            raise
        finally:
            logger.debug(f"Finished streaming file: {current_part - 1} parts delivered")

    async def clean_cache(self) -> None:
        """
        Periodically clean the file ID cache to reduce memory usage.
        Runs every 30 minutes.
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            cache_size = len(self.cached_file_ids)
            self.cached_file_ids.clear()
            logger.debug(f"Cleaned file ID cache: {cache_size} entries removed")
