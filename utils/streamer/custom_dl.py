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
from pyrogram.errors import AuthBytesInvalid, FileMigrate
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

    async def generate_media_session(self, client: Client, file_id: FileId):
        """
        Returns the appropriate session/client for streaming from the DC that contains the media file.

        SIMPLIFIED APPROACH: Reuses the main Pyrogram client which handles DC connections internally.
        The Pyrogram client automatically manages sessions for different DCs, so we don't need
        to manually create Session objects (which is incompatible with KurimuzonAkuma fork).

        Returns:
            The media session object from client.media_sessions, or the client itself for streaming
        """
        dc_id = file_id.dc_id

        # Check if we have a cached media session for this DC
        media_session = client.media_sessions.get(dc_id, None)

        if media_session is not None:
            # Validate the cached session
            logger.debug(f"Found cached media session for DC {dc_id}, validating...")

            try:
                # Basic validation: check if session has required attributes and is started
                if (hasattr(media_session, 'is_started') and
                    media_session.is_started and
                    hasattr(media_session, 'auth_key') and
                    media_session.auth_key is not None):
                    logger.info(f"Using validated cached media session for DC {dc_id}")
                    return media_session
                else:
                    logger.warning(f"Cached session for DC {dc_id} is invalid, will recreate")
                    try:
                        await media_session.stop()
                    except:
                        pass
                    del client.media_sessions[dc_id]
                    media_session = None
            except Exception as e:
                logger.warning(f"Error validating cached session for DC {dc_id}: {e}, removing from cache")
                if dc_id in client.media_sessions:
                    del client.media_sessions[dc_id]
                media_session = None

        # If no valid cached session, get or create one using the client's internal methods
        if media_session is None:
            logger.info(f"No valid cached session for DC {dc_id}, checking client's media_sessions")

            # Let Pyrogram's internal session management handle this
            # The client.media_sessions dict is managed by Pyrogram itself
            # We just need to ensure the client has access to the DC

            try:
                client_dc_id = await client.storage.dc_id()
                logger.debug(f"Client DC: {client_dc_id}, File DC: {dc_id}")

                # Pyrogram will automatically create the necessary session when we invoke methods
                # We can trigger this by getting the session from the client's internal pool
                if dc_id not in client.media_sessions:
                    logger.info(f"DC {dc_id} not in media_sessions, will be created on first invoke")

                # Return the client itself - Pyrogram will handle DC routing internally
                # when we call invoke() on it during streaming
                logger.info(f"Reusing main client for media streaming on DC {dc_id}")
                return client

            except Exception as e:
                logger.error(f"Error checking DC info: {e}")
                # Fall back to using the client directly
                logger.info(f"Falling back to main client for DC {dc_id} streaming")
                return client

        return media_session

    async def handle_dc_migration(self, client: Client, target_dc: int, file_id: FileId):
        """
        Handle FILE_MIGRATE error by switching to the correct DC.

        Args:
            client: Main Pyrogram client
            target_dc: The DC number to migrate to
            file_id: File identifier (will be updated with new DC)

        Returns:
            Media session for the target DC
        """
        logger.info(f"FileMigrate detected to DC {target_dc} - switching session")

        # Update the file_id's DC
        file_id.dc_id = target_dc

        # Invalidate old cached session if exists
        if target_dc in client.media_sessions:
            try:
                old_session = client.media_sessions[target_dc]
                if hasattr(old_session, 'stop'):
                    await old_session.stop()
            except:
                pass
            del client.media_sessions[target_dc]

        # Generate new session for target DC
        media_session = await self.generate_media_session(client, file_id)

        logger.info(f"Successfully migrated to DC {target_dc}")
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
            import re

            # Fetch first chunk with FileMigrate handling
            max_retries = 3
            r = None

            for attempt in range(max_retries):
                try:
                    r = await media_session.invoke(
                        raw.functions.upload.GetFile(
                            location=location, offset=offset, limit=chunk_size
                        ),
                    )
                    break
                except FileMigrate as e:
                    # Extract target DC from error message
                    # Error format: "The file currently being accessed is stored in DC1"
                    error_msg = str(e)
                    target_dc = None

                    # Try to extract DC number from error message
                    match = re.search(r'DC[_\s]?(\d+)', error_msg, re.IGNORECASE)
                    if match:
                        target_dc = int(match.group(1))

                    if not target_dc:
                        # Fallback: try to get from exception attributes
                        if hasattr(e, 'value'):
                            target_dc = e.value
                        elif hasattr(e, 'x'):
                            target_dc = e.x

                    if not target_dc:
                        logger.error(f"Could not extract target DC from FileMigrate error: {error_msg}")
                        raise

                    logger.info(f"FileMigrate detected to DC {target_dc} - switching session (attempt {attempt + 1}/{max_retries})")

                    # Handle DC migration
                    media_session = await self.handle_dc_migration(client, target_dc, file_id)

                    # Update location with new file_id
                    location = await self.get_location(file_id)

                    logger.info(f"Successfully migrated to DC {target_dc}, retrying GetFile")

                    # Continue to next iteration to retry with new session
                    continue

            if r is None:
                raise Exception(f"Failed to fetch first chunk after {max_retries} migration attempts")

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

                    # Fetch next chunk with FileMigrate handling
                    for attempt in range(max_retries):
                        try:
                            r = await media_session.invoke(
                                raw.functions.upload.GetFile(
                                    location=location, offset=offset, limit=chunk_size
                                ),
                            )
                            break
                        except FileMigrate as e:
                            # Extract target DC from error message
                            error_msg = str(e)
                            target_dc = None

                            # Try to extract DC number from error message
                            match = re.search(r'DC[_\s]?(\d+)', error_msg, re.IGNORECASE)
                            if match:
                                target_dc = int(match.group(1))

                            if not target_dc:
                                # Fallback: try to get from exception attributes
                                if hasattr(e, 'value'):
                                    target_dc = e.value
                                elif hasattr(e, 'x'):
                                    target_dc = e.x

                            if not target_dc:
                                logger.error(f"Could not extract target DC from FileMigrate error: {error_msg}")
                                raise

                            logger.info(f"FileMigrate detected to DC {target_dc} - switching session (attempt {attempt + 1}/{max_retries})")

                            # Handle DC migration
                            media_session = await self.handle_dc_migration(client, target_dc, file_id)

                            # Update location with new file_id
                            location = await self.get_location(file_id)

                            logger.info(f"Successfully migrated to DC {target_dc}, retrying GetFile")

                            # Continue to next iteration to retry with new session
                            continue

                    if r is None:
                        raise Exception(f"Failed to fetch chunk at part {current_part} after {max_retries} migration attempts")
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
