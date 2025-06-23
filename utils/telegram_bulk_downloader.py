import re
import asyncio
from pyrogram import Client
from pyrogram.types import Message
from utils.logger import Logger
from utils.clients import get_client
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL
import urllib.parse

logger = Logger(__name__)

BULK_DOWNLOAD_PROGRESS = {}
STOP_BULK_DOWNLOAD = []

class TelegramBulkDownloader:
    def __init__(self):
        self.client = None
        
    async def get_client(self):
        if not self.client:
            self.client = get_client()
        return self.client
    
    def parse_telegram_urls(self, urls_text):
        """Parse telegram URLs and extract channel/chat info and message IDs"""
        # Pattern to match Telegram URLs
        pattern = r'https://t\.me/([^/]+)/(\d+)'
        matches = re.findall(pattern, urls_text)
        
        if not matches:
            raise ValueError("No valid Telegram URLs found")
        
        # Group by channel/chat
        channels = {}
        for channel, msg_id in matches:
            if channel not in channels:
                channels[channel] = []
            channels[channel].append(int(msg_id))
        
        return channels
    
    def parse_url_range(self, url_range_text):
        """Parse URL range like 'https://t.me/channel/100 to https://t.me/channel/200'"""
        # Pattern to match range format
        range_pattern = r'https://t\.me/([^/]+)/(\d+)\s+to\s+https://t\.me/([^/]+)/(\d+)'
        match = re.search(range_pattern, url_range_text, re.IGNORECASE)
        
        if not match:
            # Try single URL or multiple URLs separated by newlines/spaces
            return self.parse_telegram_urls(url_range_text)
        
        start_channel, start_id, end_channel, end_id = match.groups()
        
        if start_channel != end_channel:
            raise ValueError("Start and end URLs must be from the same channel")
        
        start_id, end_id = int(start_id), int(end_id)
        
        if start_id > end_id:
            start_id, end_id = end_id, start_id
        
        # Generate all message IDs in range
        message_ids = list(range(start_id, end_id + 1))
        
        return {start_channel: message_ids}
    
    async def resolve_channel_id(self, channel_username):
        """Resolve channel username to channel ID"""
        try:
            client = await self.get_client()
            
            # Try to get chat info
            if channel_username.startswith('@'):
                channel_username = channel_username[1:]
            
            chat = await client.get_chat(channel_username)
            return chat.id
        except Exception as e:
            logger.error(f"Failed to resolve channel {channel_username}: {e}")
            raise ValueError(f"Cannot access channel @{channel_username}. Make sure the channel is public or the bot has access.")
    
    async def download_messages_from_channel(self, channel_username, message_ids, upload_id, path):
        """Download messages from a specific channel"""
        try:
            client = await self.get_client()
            channel_id = await self.resolve_channel_id(channel_username)
            
            total_messages = len(message_ids)
            processed = 0
            successful = 0
            failed = 0
            
            logger.info(f"Starting bulk download from @{channel_username}: {total_messages} messages")
            
            for msg_id in message_ids:
                if upload_id in STOP_BULK_DOWNLOAD:
                    logger.info(f"Bulk download {upload_id} stopped by user")
                    break
                
                try:
                    # Update progress
                    BULK_DOWNLOAD_PROGRESS[upload_id] = {
                        'status': 'downloading',
                        'current': processed,
                        'total': total_messages,
                        'successful': successful,
                        'failed': failed,
                        'current_message': msg_id
                    }
                    
                    # Get the message
                    message = await client.get_messages(channel_id, msg_id)
                    
                    if message and not message.empty:
                        # Check if message has media
                        media = None
                        if message.document:
                            media = message.document
                        elif message.video:
                            media = message.video
                        elif message.audio:
                            media = message.audio
                        elif message.photo:
                            media = message.photo
                        elif message.sticker:
                            media = message.sticker
                        
                        if media:
                            # Copy message to storage channel
                            copied_message = await message.copy(STORAGE_CHANNEL)
                            
                            # Get the copied media
                            copied_media = (
                                copied_message.document or 
                                copied_message.video or 
                                copied_message.audio or 
                                copied_message.photo or 
                                copied_message.sticker
                            )
                            
                            # Add to drive data
                            file_name = getattr(copied_media, 'file_name', None)
                            if not file_name:
                                # Generate filename for photos and other media without names
                                if copied_message.photo:
                                    file_name = f"photo_{msg_id}.jpg"
                                elif copied_message.sticker:
                                    file_name = f"sticker_{msg_id}.webp"
                                else:
                                    file_name = f"file_{msg_id}"
                            
                            DRIVE_DATA.new_file(
                                path,
                                file_name,
                                copied_message.id,
                                copied_media.file_size
                            )
                            
                            successful += 1
                            logger.info(f"Successfully processed message {msg_id}: {file_name}")
                        else:
                            logger.warning(f"Message {msg_id} has no media content")
                            failed += 1
                    else:
                        logger.warning(f"Message {msg_id} not found or empty")
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process message {msg_id}: {e}")
                    failed += 1
                
                processed += 1
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
            
            # Final progress update
            BULK_DOWNLOAD_PROGRESS[upload_id] = {
                'status': 'completed',
                'current': processed,
                'total': total_messages,
                'successful': successful,
                'failed': failed,
                'current_message': None
            }
            
            logger.info(f"Bulk download completed. Processed: {processed}, Successful: {successful}, Failed: {failed}")
            
        except Exception as e:
            logger.error(f"Bulk download failed: {e}")
            BULK_DOWNLOAD_PROGRESS[upload_id] = {
                'status': 'error',
                'current': 0,
                'total': len(message_ids),
                'successful': 0,
                'failed': len(message_ids),
                'error': str(e)
            }
    
    async def start_bulk_download(self, urls_text, upload_id, path):
        """Start bulk download process"""
        try:
            # Parse URLs or URL range
            channels_data = self.parse_url_range(urls_text)
            
            total_messages = sum(len(msg_ids) for msg_ids in channels_data.values())
            
            if total_messages == 0:
                raise ValueError("No valid message IDs found")
            
            if total_messages > 1000:  # Limit for safety
                raise ValueError(f"Too many messages ({total_messages}). Maximum allowed is 1000.")
            
            logger.info(f"Starting bulk download: {total_messages} messages from {len(channels_data)} channels")
            
            # Process each channel
            for channel_username, message_ids in channels_data.items():
                if upload_id in STOP_BULK_DOWNLOAD:
                    break
                    
                await self.download_messages_from_channel(
                    channel_username, 
                    message_ids, 
                    upload_id, 
                    path
                )
            
        except Exception as e:
            logger.error(f"Bulk download initialization failed: {e}")
            BULK_DOWNLOAD_PROGRESS[upload_id] = {
                'status': 'error',
                'current': 0,
                'total': 0,
                'successful': 0,
                'failed': 0,
                'error': str(e)
            }

# Global instance
telegram_bulk_downloader = TelegramBulkDownloader()

async def start_bulk_telegram_download(urls_text, upload_id, path):
    """Start bulk telegram download"""
    await telegram_bulk_downloader.start_bulk_download(urls_text, upload_id, path)

def get_bulk_download_progress(upload_id):
    """Get bulk download progress"""
    return BULK_DOWNLOAD_PROGRESS.get(upload_id, {
        'status': 'not_found',
        'current': 0,
        'total': 0,
        'successful': 0,
        'failed': 0
    })

def stop_bulk_download(upload_id):
    """Stop bulk download"""
    if upload_id not in STOP_BULK_DOWNLOAD:
        STOP_BULK_DOWNLOAD.append(upload_id)