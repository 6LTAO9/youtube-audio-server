import os
import tempfile
import time
import threading
import uuid
import gc
import psutil
import requests
import random
from flask import Flask, request, send_file, jsonify
import yt_dlp
from pathlib import Path
import logging
from functools import wraps

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Optimized settings for better iOS app connection
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024  # Increased to 150MB
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300

# Global variables for tracking
download_status = {}
download_files = {}
active_downloads = 0
MAX_CONCURRENT_DOWNLOADS = 4  # Increased from 2

# Improved rate limiting storage with better cleanup
rate_limit_storage = {}
RATE_LIMIT_CLEANUP_INTERVAL = 300  # Clean every 5 minutes

# Proxy management with better error handling
current_proxy = None
proxy_last_fetched = 0
PROXY_UPDATE_INTERVAL = 1800  # 30 minutes (more frequent updates)
proxy_failure_count = 0
MAX_PROXY_FAILURES = 3

def cleanup_rate_limit_storage():
    """Clean old rate limit entries"""
    global rate_limit_storage
    now = time.time()
    window = 600  # 10 minutes
    
    for client_ip in list(rate_limit_storage.keys()):
        if client_ip in rate_limit_storage:
            rate_limit_storage[client_ip] = [
                timestamp for timestamp in rate_limit_storage[client_ip]
                if now - timestamp < window
            ]
            if not rate_limit_storage[client_ip]:
                del rate_limit_storage[client_ip]

def get_working_proxy():
    """Simplified proxy function - returns None to use direct connection"""
    global current_proxy
    # Use direct connection to avoid complications
    current_proxy = None
    return None

def test_proxy_comprehensive(proxy_url, timeout=8):
    """Simplified proxy test"""
    return False

def rate_limit(max_requests=15, window=300):  # More generous: 15 requests per 5 minutes
    """Improved rate limiting with better iOS app support"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            now = time.time()
            
            # Get or create client record
            if client_ip not in rate_limit_storage:
                rate_limit_storage[client_ip] = []
            
            # Clean old entries for this client
            rate_limit_storage[client_ip] = [
                timestamp for timestamp in rate_limit_storage[client_ip]
                if now - timestamp < window
            ]
            
            # Check rate limit with some tolerance
            current_requests = len(rate_limit_storage[client_ip])
            if current_requests >= max_requests:
                # Add some jitter to avoid thundering herd
                retry_after = window + random.randint(10, 60)
                logger.warning(f"Rate limit exceeded for {client_ip}: {current_requests}/{max_requests}")
                return jsonify({
                    "error": "Rate limit exceeded. Please wait before trying again.",
                    "retry_after": retry_after,
                    "current_requests": current_requests,
                    "limit": max_requests
                }), 429
            
            # Add current request
            rate_limit_storage[client_ip].append(now)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def check_memory_usage():
    """Less aggressive memory checking"""
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 85:  # Increased threshold from 80%
            logger.warning(f"High memory usage: {memory_percent}%")
            cleanup_old_downloads(force=True)
            gc.collect()
            return True
    except Exception as e:
        logger.error(f"Memory check failed: {e}")
    return False

def cleanup_old_downloads(force=False, max_age=2400):  # Increased to 40 minutes
    """Less aggressive cleanup"""
    try:
        current_time = time.time()
        to_delete = []
        
        for job_id, status_info in download_status.items():
            age_limit = 600 if force else max_age  # 10 min if forced, 40 min otherwise
            
            if current_time - status_info.get('created_at', current_time) > age_limit:
                to_delete.append(job_id)
        
        for job_id in to_delete:
            cleanup_download(job_id, silent=True)
        
        if to_delete:
            logger.info(f"Cleaned up {len(to_delete)} old downloads")
            
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def check_system_resources():
    """More lenient resource checking"""
    global active_downloads
    
    if active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        return False, f"Server busy ({active_downloads}/{MAX_CONCURRENT_DOWNLOADS} downloads active). Try again in a moment."
    
    # Less aggressive memory checking
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 90:  # Only fail at 90%
            cleanup_old_downloads(force=True)
            time.sleep(1)  # Brief pause for cleanup
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 95:
                return False, "Server under heavy load. Please try again in a few minutes."
    except Exception as e:
        logger.error(f"Memory check failed: {e}")
    
    # Less aggressive disk checking
    try:
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        if free_gb < 0.3:  # Only fail below 300MB
            cleanup_old_downloads(force=True)
            time.sleep(1)
            disk_usage = psutil.disk_usage('/tmp')
            free_gb = disk_usage.free / (1024**3)
            if free_gb < 0.1:
                return False, "Insufficient storage space. Please try again later."
    except Exception as e:
        logger.error(f"Disk check failed: {e}")
    
    return True, "OK"

@app.route('/', methods=['GET'])
def health_check():
    """Enhanced health check"""
    try:
        memory_percent = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        
        status = {
            "status": "healthy",
            "service": "youtube-audio-downloader",
            "version": "2.0-ios-optimized",
            "active_downloads": active_downloads,
            "max_concurrent": MAX_CONCURRENT_DOWNLOADS,
            "memory_usage": f"{memory_percent:.1f}%",
            "free_disk_gb": f"{free_gb:.2f}",
            "total_jobs": len(download_status),
            "proxy_status": "disabled",
            "rate_limit_clients": len(rate_limit_storage),
            "uptime": time.time() - start_time if 'start_time' in globals() else 0
        }
        
        if memory_percent > 95 or free_gb < 0.1:
            status["status"] = "degraded"
            status["warning"] = "High resource usage"
            
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "healthy", 
            "service": "youtube-audio-downloader",
            "error": "health_check_partial_failure"
        }), 200

@app.route('/download/audio/fast', methods=['POST'])
@rate_limit(max_requests=12, window=300)  # 12 requests per 5 minutes for main endpoint
def download_audio_fast():
    """Optimized fast download for iOS app"""
    global active_downloads
    
    # Quick resource check
    can_proceed, message = check_system_resources()
    if not can_proceed:
        logger.warning(f"Resource check failed: {message}")
        return jsonify({"error": message, "code": "RESOURCE_LIMIT"}), 503
    
    if not request.is_json:
        return jsonify({"error": "Request must be JSON", "code": "INVALID_FORMAT"}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided", "code": "MISSING_URL"}), 400

    # Clean URL
    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    logger.info(f"Fast download request: {youtube_url}")
    
    active_downloads += 1
    temp_dir = None
    
    try:
        # Create temp directory
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_fast_')
        
        # Cookie handling
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')

        # Simple, reliable settings for iOS app
        ydl_opts = {
            'format': 'bestaudio[abr<=160]/bestaudio[ext=m4a]/bestaudio',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '160',
            }],
            'postprocessor_args': [
                '-ar', '44100',
                '-ac', '2',
                '-b:a', '160k',
                '-threads', '2',
                '-preset', 'fast',
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            
            # Network settings optimized for reliability
            'concurrent_fragment_downloads': 3,
            'http_chunk_size': 1048576,  # 1MB chunks
            'buffer_size': 32768,
            'no_color': True,
            'quiet': True,
            'no_warnings': True,
            
            # More forgiving retry settings
            'socket_timeout': 20,
            'fragment_retries': 2,
            'retries': 2,
            'extractor_retries': 1,
            
            # Skip unnecessary operations
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
            'extract_flat': False,
        }

        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])
                
                # Find and return file
                for pattern in ['*.mp3', '*.m4a', '*.webm', '*.opus']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        
                        # Get file info for better filename
                        file_size = os.path.getsize(file_path)
                        timestamp = int(time.time())
                        safe_filename = f"audio_{timestamp}.mp3"
                        
                        logger.info(f"Download successful: {file_size} bytes")
                        
                        def cleanup_after_send():
                            time.sleep(45)  # Wait longer before cleanup
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except Exception as e:
                                logger.error(f"Cleanup error: {e}")
                        
                        cleanup_thread = threading.Thread(target=cleanup_after_send)
                        cleanup_thread.daemon = True
                        cleanup_thread.start()
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                
                return jsonify({
                    "error": "Download completed but no audio file was created",
                    "code": "NO_OUTPUT_FILE"
                }), 500
        
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle specific errors
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({
                    "error": "YouTube rate limit exceeded. Please wait a few minutes.",
                    "code": "YOUTUBE_RATE_LIMIT"
                }), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
                return jsonify({
                    "error": "Video is unavailable, private, or has been removed",
                    "code": "VIDEO_UNAVAILABLE"
                }), 400
            elif any(phrase in error_str for phrase in ["not available in your country", "geo", "region", "blocked in your country"]):
                return jsonify({
                    "error": "Video not available in server region",
                    "code": "GEO_BLOCKED"
                }), 400
            elif "copyright" in error_str:
                return jsonify({
                    "error": "Video blocked due to copyright restrictions",
                    "code": "COPYRIGHT_BLOCKED"
                }), 400
            else:
                logger.error(f"Download failed: {e}")
                return jsonify({
                    "error": "Download failed due to server error",
                    "code": "DOWNLOAD_FAILED",
                    "details": str(e)[:200]  # Truncate long error messages
                }), 500
    
    finally:
        active_downloads -= 1
        # Cleanup temp directory if still exists
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")

# Keep compatibility with existing endpoints
@app.route('/download/audio', methods=['POST'])
@rate_limit(max_requests=10, window=300)
def download_audio():
    """Original endpoint - redirect to fast"""
    return download_audio_fast()

@app.route('/download/audio/ultrafast', methods=['POST'])
@rate_limit(max_requests=8, window=300)
def download_audio_ultrafast():
    """Ultra-fast download optimized for speed while maintaining good quality"""
    global active_downloads
    
    can_proceed, message = check_system_resources()
    if not can_proceed:
        return jsonify({"error": message, "code": "RESOURCE_LIMIT"}), 503
    
    if not request.is_json:
        return jsonify({"error": "Request must be JSON", "code": "INVALID_FORMAT"}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided", "code": "MISSING_URL"}), 400

    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    logger.info(f"Ultra-fast download request: {youtube_url}")
    
    active_downloads += 1
    temp_dir = None
    
    try:
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_ultra_')
        
        # Premium speed-optimized settings
        ydl_opts = {
            'format': 'bestaudio[ext=m4a][abr<=320]/bestaudio[abr<=320]/bestaudio',  # Premium quality
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',  # Maximum MP3 quality
            }],
            'postprocessor_args': [
                '-ar', '48000',              # High sample rate
                '-ac', '2',                  # Stereo
                '-b:a', '320k',              # Maximum bitrate
                '-threads', '0',             # Use all CPU threads
                '-preset', 'faster',         # Speed-optimized but high quality
                '-movflags', '+faststart',
                '-map_metadata', '-1',       # Strip metadata for speed
                '-fflags', '+bitexact+fastseek',
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            
            # Extreme network performance
            'concurrent_fragment_downloads': 20,  # Maximum concurrent downloads
            'http_chunk_size': 16777216,          # 16MB chunks - absolute maximum
            'buffer_size': 1048576,               # 1MB buffer
            'no_color': True,
            'quiet': True,
            'no_warnings': True,
            
            # Zero tolerance for delays
            'socket_timeout': 25,
            'fragment_retries': 0,
            'retries': 0,
            'extractor_retries': 1,
            
            # All speed optimizations
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
            'no_check_certificate': True,
            'prefer_insecure': True,             # Skip HTTPS when possible
            
            # Advanced network optimization
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Accept': '*/*',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
            }
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract video info first to get the title
                info = ydl.extract_info(youtube_url, download=False)
                video_title = info.get('title', 'Unknown Video')
                
                # Clean the title for filename use
                safe_title = "".join(c for c in video_title if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_title = safe_title[:50]  # Limit to 50 characters
                if not safe_title:
                    safe_title = "Downloaded Audio"
                
                # Download the video
                ydl.download([youtube_url])
                
                for pattern in ['*.mp3', '*.m4a', '*.webm', '*.opus']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        
                        # Use actual video title for filename
                        safe_filename = f"{safe_title}.mp3"
                        
                        file_size = os.path.getsize(file_path)
                        logger.info(f"Ultra-fast download successful: {file_size} bytes - '{video_title}'")
                        
                        def cleanup_after_send():
                            time.sleep(30)
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except Exception as e:
                                logger.error(f"Cleanup error: {e}")
                        
                        cleanup_thread = threading.Thread(target=cleanup_after_send)
                        cleanup_thread.daemon = True
                        cleanup_thread.start()
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                
                return jsonify({
                    "error": "Download completed but no audio file was created",
                    "code": "NO_OUTPUT_FILE"
                }), 500
        
        except Exception as e:
            error_str = str(e).lower()
            
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({
                    "error": "YouTube rate limit exceeded. Please wait a few minutes.",
                    "code": "YOUTUBE_RATE_LIMIT"
                }), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
                return jsonify({
                    "error": "Video is unavailable, private, or has been removed",
                    "code": "VIDEO_UNAVAILABLE"
                }), 400
            else:
                logger.error(f"Ultra-fast download failed: {e}")
                return jsonify({
                    "error": "Download failed",
                    "code": "DOWNLOAD_FAILED"
                }), 500
    
    finally:
        active_downloads -= 1
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")

@app.route('/server/stats', methods=['GET'])
def server_stats():
    """Detailed server statistics for debugging"""
    try:
        return jsonify({
            "active_downloads": active_downloads,
            "max_concurrent": MAX_CONCURRENT_DOWNLOADS,
            "total_jobs": len(download_status),
            "rate_limit_clients": len(rate_limit_storage),
            "proxy_status": "disabled",
            "memory_percent": psutil.virtual_memory().percent,
            "disk_free_gb": psutil.disk_usage('/tmp').free / (1024**3),
            "uptime": time.time() - start_time if 'start_time' in globals() else 0
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def periodic_cleanup():
    """Improved periodic cleanup"""
    while True:
        try:
            time.sleep(300)  # Every 5 minutes
            cleanup_old_downloads()
            cleanup_rate_limit_storage()
            gc.collect()
            
            # Log periodic stats
            if len(download_status) > 0:
                logger.info(f"Periodic cleanup: {len(download_status)} active jobs, {active_downloads} downloads")
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {e}")
    return jsonify({
        "error": "Internal server error", 
        "code": "INTERNAL_ERROR"
    }), 500

@app.errorhandler(413)
def handle_file_too_large(e):
    return jsonify({
        "error": "File too large",
        "limit": "150MB maximum",
        "code": "FILE_TOO_LARGE"
    }), 413

@app.errorhandler(404)
def handle_not_found(e):
    return jsonify({
        "error": "Endpoint not found",
        "code": "NOT_FOUND",
        "available_endpoints": [
            "POST /download/audio/fast",
            "GET /",
            "GET /server/stats"
        ]
    }), 404

def cleanup_download(job_id, silent=False):
    """Clean up download files and status"""
    try:
        if job_id in download_files:
            file_path = download_files[job_id]
            if os.path.exists(file_path):
                parent_dir = os.path.dirname(file_path)
                import shutil
                shutil.rmtree(parent_dir, ignore_errors=True)
            del download_files[job_id]
        
        if job_id in download_status:
            del download_status[job_id]
            
        if not silent:
            logger.info(f"Cleaned up download {job_id}")
    except Exception as e:
        if not silent:
            logger.error(f"Cleanup error for {job_id}: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    
    # Set start time for uptime tracking
    start_time = time.time()
    
    # Environment optimizations
    os.environ['FFMPEG_THREADS'] = '2'
    os.environ['MALLOC_ARENA_MAX'] = '2'
    
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    # System checks
    try:
        import subprocess
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True, text=True)
        logger.info("✓ FFmpeg available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("✗ FFmpeg not found")
    
    try:
        logger.info(f"✓ yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.warning("Could not determine yt-dlp version")
    
    logger.info("=== YouTube Audio Downloader Server ===")
    logger.info("📱 OPTIMIZED FOR iOS APP CONNECTION")
    logger.info("🚀 Enhanced for Render.com FREE TIER")
    logger.info("")
    logger.info("📱 iOS App Endpoints:")
    logger.info("- POST /download/audio/fast (main endpoint - 160kbps)")
    logger.info("- POST /download/audio/ultrafast (fastest - 128kbps)")  
    logger.info("- GET  / (health check)")
    logger.info("- GET  /server/stats (debugging)")
    logger.info("")
    logger.info("⚡ iOS Optimizations:")
    logger.info(f"- Rate limit: 12 requests/5min")
    logger.info(f"- Max concurrent: {MAX_CONCURRENT_DOWNLOADS}")
    logger.info("- Better error codes and messages")
    logger.info("- Direct connection (no proxy complications)")
    logger.info("- Less aggressive resource monitoring")
    logger.info("- Keep-alive ping every hour (prevents sleep)")
    logger.info("")
    
    logger.info("🌐 Proxy: DISABLED (direct connection only)")
    
    if os.environ.get('RENDER'):
        logger.info("🔥 Running on Render.com")
    
    # Run with improved settings
    app.run(
        host='0.0.0.0', 
        port=port, 
        debug=False, 
        threaded=True,
        use_reloader=False
    )
