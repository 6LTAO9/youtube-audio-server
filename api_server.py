if "requested format is not available" in error_str:
                logger.warning("Format not available for ultrafast, trying basic fallback...")
                
                # Most basic fallback with enhanced stealth
                fallback_opts = {
                    'format': 'worst',  # Most basic fallback
                    'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '64',  # Very low quality
                    }],
                    'prefer_ffmpeg': True,
                    'keepvideo': False,
                    'noplaylist': True,
                    'geo_bypass': True,
                    'color': 'never',  # Fix compatibility issue
                    'quiet': True,
                    'no_warnings': True,
                    'socket_timeout': 30,
                    'retries': 1,
                    'fragment_retries': 1,
                    'extractor_retries': 1,
                    'sleep_interval': 3,
                    'max_sleep_interval': 6,
                    'user_agent': 'Mozilla/5.0 (Android 10; Mobile; rv:91.0) Gecko/91.0 Firefox/91.0',
                    'http_headers': {
                        'User-Agent': 'Mozilla/5.0 (Android 10; Mobile; rv:91.0) Gecko/91.0 Firefox/91.0',
                        'Accept': '*/*',
                        'Accept-Language': 'en-US,en;q=0.5',
                    },
                    'youtube_include_dash_manifest': False,
                    'youtube_include_hls_manifest': False,
                }
                
                if proxy_url:
                    fallback_opts['proxy'] = proxy_url
                    
                try:
                    # Longer delay for fallback
                    time.sleep(random.uniform(3, 6))
                    
                    with yt_dlp.YoutubeDL(fallback_opts) as ydl_fallback:
                        ydl_fallback.download([youtube_url])
                        
                        for pattern in audio_extensions:
                            found_files = list(Path(temp_dir).glob(pattern))
                            if found_files:
                                found_file = str(found_files[0])
                                timestamp = int(time.time())
                                safe_filename = f"audio_fast_{timestamp}.mp3"
                                
                                def cleanup_after_send():
                                    time.sleep(45)
                                    try:
                                        import shutil
                                        shutil.rmtree(temp_dir, ignore_errors=True)
                                    except Exception as e:
                                        logger.error(f"Cleanup error: {e}")
                                
                                cleanup_thread = threading.Thread(target=cleanup_after_send)
                                cleanup_thread.daemon = True
                                cleanup_thread.start()
                                
                                logger.info("Ultrafast fallback download successful")
                                return send_file(
                                    found_file, 
                                    as_attachment=True, 
                                    download_name=safe_filename,
                                    mimetype='audio/mpeg'
                                )
                        
                        return jsonify({
                            "error": "Fallback download completed but no audio file was created",
                            "code": "NO_OUTPUT_FILE"
                        }), 500
                        
                except Exception as fallback_error:
                    fallback_error_str = str(fallback_error).lower()
                    if "429" in fallback_error_str or "too many requests" in fallback_error_str:
                        return jsonify({
                            "error": "YouTube rate limit exceeded. Server heavily throttled. Please wait 15+ minutes.",
                            "code": "YOUTUBE_RATE_LIMIT_SEVERE",
                            "retry_after": 1200
                        }), 429
                    elif "precondition check failed" in fallback_error_str:
                        return jsonify({
                            "error": "YouTube anti-bot protection active. Please try again much later.",
                            "code": "YOUTUBE_BLOCKED_SEVERE",
                            "retry_after": 2400
                        }), 403
                    else:
                        logger.error(f"Ultrafast fallback also failed: {fallback_error}")
                        return jsonify({
                            "error": "Video format not supported - tried multiple formats",
                            "code": "FORMAT_NOT_SUPPORTED"
                        }), 400
            
            # Handle other errors similar to main function with enhanced messaging
            elif "429" in error_str or "too many requests" in error_str:
                return jsonify({
                    "error": "YouTube rate limit exceeded. Server is being heavily throttled. Please wait 15+ minutes.",
                    "code": "YOUTUBE_RATE_LIMIT",
                    "retry_after": 900,
                    "suggestion": "Try again much later or use a different video"
                }), 429
            elif "precondition check failed" in error_str:
                return jsonify({
                    "error": "YouTube anti-bot protection triggered. Please wait before trying again.",
                    "code": "YOUTUBE_ANTI_BOT", 
                    "retry_after": 1800,
                    "suggestion": "Server detected as automated - wait longer between requests"
                }), 403
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
                return jsonify({
                    "error": "Video is unavailable, private, or has been removed",
                    "code": "VIDEO_UNAVAILABLE"
                }), 400
            elif "only images are available" in error_str:
                return jsonify({
                    "error": "This content only contains images, no audio available",
                    "code": "NO_AUDIO_CONTENT"
                }), 400
            else:
                logger.error(f"Ultrafast download failed: {e}")
                return jsonify({
                    "error": "Download failed due to server error",
                    "code": "DOWNLOAD_FAILED",
                    "details": str(e)[:200]
                }), 500
    
    finally:
        active_downloads -= 1
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")

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
from datetime import datetime, timedelta

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
    """Improved proxy fetching with better error handling"""
    global current_proxy, proxy_last_fetched, proxy_failure_count
    
    current_time = time.time()
    
    # Skip proxy if too many failures
    if proxy_failure_count >= MAX_PROXY_FAILURES:
        logger.info("Skipping proxy due to repeated failures, using direct connection")
        current_proxy = None
        return None
    
    # Check if we need to update proxy
    if (current_time - proxy_last_fetched > PROXY_UPDATE_INTERVAL) or (current_proxy is None):
        try:
            # Multiple proxy sources for better reliability
            proxy_urls = [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
                'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt'
            ]
            
            all_proxies = []
            for url in proxy_urls:
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        proxies = [line.strip() for line in response.text.strip().split('\n') 
                                 if ':' in line.strip() and line.strip().count(':') >= 1]
                        all_proxies.extend(proxies[:20])  # Limit to first 20 from each source
                except Exception as e:
                    logger.warning(f"Failed to fetch from {url}: {e}")
                    continue
            
            if all_proxies:
                # Test fewer proxies but more thoroughly
                test_proxies = random.sample(all_proxies, min(5, len(all_proxies)))
                for proxy in test_proxies:
                    if ':' in proxy:
                        test_proxy = f"http://{proxy}"
                        if test_proxy_quick(test_proxy):
                            current_proxy = test_proxy
                            proxy_last_fetched = current_time
                            proxy_failure_count = 0  # Reset failure count
                            logger.info(f"Found working proxy: {current_proxy}")
                            return current_proxy
                
                logger.warning("No working proxies found")
                current_proxy = None
            else:
                logger.warning("No proxies fetched")
                current_proxy = None
                
        except Exception as e:
            logger.error(f"Error fetching proxy: {e}")
            current_proxy = None
    
    return current_proxy

def test_proxy_quick(proxy_url, timeout=8):
    """Quick proxy test with better timeout"""
    try:
        proxies = {'http': proxy_url, 'https': proxy_url}
        response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=timeout)
        return response.status_code == 200
    except:
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
            "version": "2.1-ios-optimized-fixed",
            "active_downloads": active_downloads,
            "max_concurrent": MAX_CONCURRENT_DOWNLOADS,
            "memory_usage": f"{memory_percent:.1f}%",
            "free_disk_gb": f"{free_gb:.2f}",
            "total_jobs": len(download_status),
            "proxy_status": "active" if current_proxy else "direct",
            "proxy_failures": proxy_failure_count,
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
    """Optimized fast download for iOS app with improved format selection"""
    global active_downloads, proxy_failure_count
    
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
        
        # Get proxy with better error handling
        proxy_url = get_working_proxy()

        # IMPROVED: Much more flexible format selection
        ydl_opts = {
            # More flexible format selection - will try multiple formats
            'format': (
                'bestaudio[ext=m4a][abr<=160]/'  # Prefer m4a under 160kbps
                'bestaudio[ext=m4a]/'            # Any m4a
                'bestaudio[abr<=160]/'           # Any format under 160kbps  
                'bestaudio[ext=webm][abr<=160]/' # WebM under 160kbps
                'bestaudio[ext=opus][abr<=160]/' # Opus under 160kbps
                'bestaudio[ext=mp4]/'            # MP4 audio
                'bestaudio'                      # Fall back to best available
            ),
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
            
            # Geo-blocking bypass options
            'geo_bypass': True,
            'geo_bypass_country': 'US',
            'geo_bypass_ip_block': None,
            
            # Network settings optimized for reliability
            'concurrent_fragment_downloads': 2,  # Reduced from 3 for stability
            'http_chunk_size': 1048576,
            'buffer_size': 32768,
            'no_color': True,
            'quiet': False,  # Changed to False for better debugging
            'no_warnings': False,  # Changed to False for better debugging
            
            # More forgiving retry settings
            'socket_timeout': 25,  # Increased timeout
            'fragment_retries': 3,  # Increased retries
            'retries': 3,          # Increased retries
            'extractor_retries': 2, # Increased retries
            
            # Skip unnecessary operations
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
            'extract_flat': False,
            
            # Additional reliability options
            'ignoreerrors': False,  # Don't ignore errors, we want to handle them
            'abort_on_unavailable_fragment': False,  # Don't abort on single fragment failure
        }

        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
            
        # Add proxy if available
        if proxy_url:
            ydl_opts['proxy'] = proxy_url
            logger.info(f"Using proxy: {proxy_url}")

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # First, try to extract info to validate the video
                try:
                    info = ydl.extract_info(youtube_url, download=False)
                    logger.info(f"Video info extracted: {info.get('title', 'Unknown')}")
                except Exception as info_error:
                    logger.warning(f"Could not extract info, trying direct download: {info_error}")
                
                # Now download
                ydl.download([youtube_url])
                
                # Find and return file
                audio_extensions = ['*.mp3', '*.m4a', '*.webm', '*.opus', '*.aac', '*.ogg']
                found_file = None
                
                for pattern in audio_extensions:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        found_file = str(found_files[0])
                        break
                
                if found_file:
                    # Get file info for better filename
                    file_size = os.path.getsize(found_file)
                    timestamp = int(time.time())
                    safe_filename = f"audio_{timestamp}.mp3"
                    
                    logger.info(f"Download successful: {file_size} bytes, file: {os.path.basename(found_file)}")
                    
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
                        found_file, 
                        as_attachment=True, 
                        download_name=safe_filename,
                        mimetype='audio/mpeg'
                    )
                else:
                    logger.error(f"No audio files found in {temp_dir}")
                    # List all files in temp directory for debugging
                    try:
                        all_files = list(Path(temp_dir).glob('*'))
                        logger.error(f"Files in temp dir: {[f.name for f in all_files]}")
                    except Exception:
                        pass
                    
                    return jsonify({
                        "error": "Download completed but no audio file was created",
                        "code": "NO_OUTPUT_FILE"
                    }), 500
        
        except Exception as e:
            error_str = str(e).lower()
            logger.error(f"Download error details: {e}")
            
            # Handle format-specific errors with fallback
            if "requested format is not available" in error_str:
                logger.warning("Format not available, trying with most basic settings...")
                
                # Try with most basic format selection
                fallback_opts = ydl_opts.copy()
                fallback_opts['format'] = 'worst[ext=m4a]/worst'  # Try worst quality as fallback
                fallback_opts['postprocessors'] = [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '96',  # Lower quality
                }]
                
                try:
                    with yt_dlp.YoutubeDL(fallback_opts) as ydl_fallback:
                        ydl_fallback.download([youtube_url])
                        
                        # Find and return file
                        for pattern in audio_extensions:
                            found_files = list(Path(temp_dir).glob(pattern))
                            if found_files:
                                found_file = str(found_files[0])
                                timestamp = int(time.time())
                                safe_filename = f"audio_{timestamp}.mp3"
                                
                                def cleanup_after_send():
                                    time.sleep(45)
                                    try:
                                        import shutil
                                        shutil.rmtree(temp_dir, ignore_errors=True)
                                    except Exception as e:
                                        logger.error(f"Cleanup error: {e}")
                                
                                cleanup_thread = threading.Thread(target=cleanup_after_send)
                                cleanup_thread.daemon = True
                                cleanup_thread.start()
                                
                                logger.info("Fallback download successful")
                                return send_file(
                                    found_file, 
                                    as_attachment=True, 
                                    download_name=safe_filename,
                                    mimetype='audio/mpeg'
                                )
                        
                        return jsonify({
                            "error": "Fallback download completed but no audio file was created",
                            "code": "NO_OUTPUT_FILE"
                        }), 500
                        
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed: {fallback_error}")
                    return jsonify({
                        "error": "Video format not supported - tried multiple formats",
                        "code": "FORMAT_NOT_SUPPORTED"
                    }), 400
            
            # Handle proxy-related errors with better recovery
            elif any(phrase in error_str for phrase in ["tunnel", "proxy", "connection failed", "timeout"]):
                logger.warning(f"Proxy error detected: {e}")
                proxy_failure_count += 1
                
                if proxy_failure_count < MAX_PROXY_FAILURES:
                    # Retry with direct connection
                    current_proxy = None
                    logger.info("Retrying with direct connection...")
                    
                    if 'proxy' in ydl_opts:
                        del ydl_opts['proxy']
                    
                    try:
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            ydl.download([youtube_url])
                            
                            for pattern in audio_extensions:
                                found_files = list(Path(temp_dir).glob(pattern))
                                if found_files:
                                    found_file = str(found_files[0])
                                    timestamp = int(time.time())
                                    safe_filename = f"audio_{timestamp}.mp3"
                                    
                                    def cleanup_after_send():
                                        time.sleep(45)
                                        try:
                                            import shutil
                                            shutil.rmtree(temp_dir, ignore_errors=True)
                                        except Exception as e:
                                            logger.error(f"Cleanup error: {e}")
                                    
                                    cleanup_thread = threading.Thread(target=cleanup_after_send)
                                    cleanup_thread.daemon = True
                                    cleanup_thread.start()
                                    
                                    return send_file(
                                        found_file, 
                                        as_attachment=True, 
                                        download_name=safe_filename,
                                        mimetype='audio/mpeg'
                                    )
                            
                            return jsonify({
                                "error": "Download completed but no audio file was created",
                                "code": "NO_OUTPUT_FILE"
                            }), 500
                    
                    except Exception as retry_error:
                        logger.error(f"Direct connection also failed: {retry_error}")
                        return jsonify({
                            "error": "Download failed with both proxy and direct connection",
                            "code": "CONNECTION_FAILED",
                            "details": str(retry_error)[:200]
                        }), 500
                else:
                    return jsonify({
                        "error": "Too many proxy failures, try again later",
                        "code": "PROXY_FAILED"
                    }), 503
            
            # Handle other specific errors
            elif "429" in error_str or "too many requests" in error_str:
                return jsonify({
                    "error": "YouTube rate limit exceeded. Please wait a few minutes.",
                    "code": "YOUTUBE_RATE_LIMIT"
                }), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
                return jsonify({
                    "error": "Video is unavailable, private, or has been removed",
                    "code": "VIDEO_UNAVAILABLE"
                }), 400
            elif "copyright" in error_str:
                return jsonify({
                    "error": "Video blocked due to copyright restrictions",
                    "code": "COPYRIGHT_BLOCKED"
                }), 400
            elif "sign in to confirm your age" in error_str:
                return jsonify({
                    "error": "Video requires age verification",
                    "code": "AGE_RESTRICTED"
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
@rate_limit(max_requests=8, window=300)  # Slightly more restrictive for lowest quality
def download_audio_ultrafast():
    """Ultra-fast download with lowest quality"""
    global active_downloads, proxy_failure_count
    
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
    
    logger.info(f"Ultrafast download request: {youtube_url}")
    
    active_downloads += 1
    temp_dir = None
    
    try:
        # Create temp directory
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_ultra_')
        
        # Cookie handling
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        
        # Get proxy with better error handling
        proxy_url = get_working_proxy()

        # Ultra-fast settings with lowest quality for speed
        ydl_opts = {
            'format': (
                'worst[ext=m4a]/'     # Worst quality m4a
                'worst[ext=webm]/'    # Worst quality webm
                'worst[abr<=96]/'     # Any format under 96kbps
                'worst'               # Absolute worst available
            ),
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '96',  # Lower quality for speed
            }],
            'postprocessor_args': [
                '-ar', '22050',  # Lower sample rate
                '-ac', '1',      # Mono for smaller files
                '-b:a', '96k',
                '-threads', '1', # Single thread for stability
                '-preset', 'ultrafast',
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            'geo_bypass': True,
            'geo_bypass_country': 'US',
            'concurrent_fragment_downloads': 1,  # Single thread
            'http_chunk_size': 524288,  # Smaller chunks
            'buffer_size': 16384,       # Smaller buffer
            'no_color': True,
            'quiet': False,
            'no_warnings': False,
            'socket_timeout': 20,
            'fragment_retries': 2,
            'retries': 2,
            'extractor_retries': 1,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
            'extract_flat': False,
            'ignoreerrors': False,
            'abort_on_unavailable_fragment': False,
        }

        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
            
        if proxy_url:
            ydl_opts['proxy'] = proxy_url

        # Use same download logic as the main function
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])
                
                # Find and return file
                audio_extensions = ['*.mp3', '*.m4a', '*.webm', '*.opus', '*.aac', '*.ogg']
                found_file = None
                
                for pattern in audio_extensions:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        found_file = str(found_files[0])
                        break
                
                if found_file:
                    file_size = os.path.getsize(found_file)
                    timestamp = int(time.time())
                    safe_filename = f"audio_fast_{timestamp}.mp3"
                    
                    logger.info(f"Ultrafast download successful: {file_size} bytes")
                    
                    def cleanup_after_send():
                        time.sleep(45)
                        try:
                            import shutil
                            shutil.rmtree(temp_dir, ignore_errors=True)
                        except Exception as e:
                            logger.error(f"Cleanup error: {e}")
                    
                    cleanup_thread = threading.Thread(target=cleanup_after_send)
                    cleanup_thread.daemon = True
                    cleanup_thread.start()
                    
                    return send_file(
                        found_file, 
                        as_attachment=True, 
                        download_name=safe_filename,
                        mimetype='audio/mpeg'
                    )
                else:
                    return jsonify({
                        "error": "Download completed but no audio file was created",
                        "code": "NO_OUTPUT_FILE"
                    }), 500
        
        except Exception as e:
            # Handle the same errors as the main function
            error_str = str(e).lower()
            
            if "requested format is not available" in error_str:
                logger.warning("Format not available for ultrafast, trying basic fallback...")
                
                fallback_opts = ydl_opts.copy()
                fallback_opts['format'] = 'worst'  # Most basic fallback
                
                try:
                    with yt_dlp.YoutubeDL(fallback_opts) as ydl_fallback:
                        ydl_fallback.download([youtube_url])
                        
                        for pattern in audio_extensions:
                            found_files = list(Path(temp_dir).glob(pattern))
                            if found_files:
                                found_file = str(found_files[0])
                                timestamp = int(time.time())
                                safe_filename = f"audio_fast_{timestamp}.mp3"
                                
                                def cleanup_after_send():
                                    time.sleep(45)
                                    try:
                                        import shutil
                                        shutil.rmtree(temp_dir, ignore_errors=True)
                                    except Exception as e:
                                        logger.error(f"Cleanup error: {e}")
                                
                                cleanup_thread = threading.Thread(target=cleanup_after_send)
                                cleanup_thread.daemon = True
                                cleanup_thread.start()
                                
                                logger.info("Ultrafast fallback download successful")
                                return send_file(
                                    found_file, 
                                    as_attachment=True, 
                                    download_name=safe_filename,
                                    mimetype='audio/mpeg'
                                )
                        
                        return jsonify({
                            "error": "Fallback download completed but no audio file was created",
                            "code": "NO_OUTPUT_FILE"
                        }), 500
                        
                except Exception as fallback_error:
                    logger.error(f"Ultrafast fallback also failed: {fallback_error}")
                    return jsonify({
                        "error": "Video format not supported - tried multiple formats",
                        "code": "FORMAT_NOT_SUPPORTED"
                    }), 400
            
            # Handle other errors similar to main function
            elif "429" in error_str or "too many requests" in error_str:
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
                logger.error(f"Ultrafast download failed: {e}")
                return jsonify({
                    "error": "Download failed due to server error",
                    "code": "DOWNLOAD_FAILED",
                    "details": str(e)[:200]
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
            "proxy_status": current_proxy,
            "proxy_failures": proxy_failure_count,
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
            "POST /download/audio/ultrafast", 
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
    os.environ['MALLOC_ARENA_MAX'] = '2'  # Slightly increased
    
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    # Initialize proxy on startup
    logger.info("Initializing proxy system...")
    try:
        get_working_proxy()
    except Exception as e:
        logger.warning(f"Initial proxy setup failed: {e}")
    
    # System checks
    try:
        import subprocess
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True, text=True)
        logger.info("✓ FFmpeg available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("✗ FFmpeg not found - this will cause audio conversion to fail!")
    
    try:
        logger.info(f"✓ yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.warning("Could not determine yt-dlp version")
    
    logger.info("=== YouTube Audio Downloader Server ===")
    logger.info("🍎 OPTIMIZED FOR iOS APP CONNECTION")
    logger.info("🚀 Enhanced for Render.com FREE TIER")
    logger.info("🔧 FIXED: Format selection + YouTube anti-bot issues")
    logger.info("🛡️  ANTI-DETECTION: Enhanced stealth measures")
    logger.info("")
    logger.info("📱 iOS App Endpoints:")
    logger.info("- POST /download/audio/fast (main endpoint - 160kbps)")
    logger.info("- POST /download/audio/ultrafast (fastest - 96kbps)")  
    logger.info("- GET  / (health check)")
    logger.info("- GET  /server/stats (debugging)")
    logger.info("")
    logger.info("🔧 Major Fixes Applied:")
    logger.info("- Fixed yt-dlp compatibility issues (_allow_colors error)")
    logger.info("- Enhanced anti-detection with random delays") 
    logger.info("- Better rate limit and anti-bot error handling")
    logger.info("- Multiple fallback strategies for blocked requests")
    logger.info("- Mobile user agents and headers for stealth")
    logger.info("- Conservative retry settings to avoid detection")
    logger.info("")
    logger.info("⚡ Enhanced Features:")
    logger.info(f"- Rate limit: 12 requests/5min for fast, 8 for ultrafast")
    logger.info(f"- Max concurrent: {MAX_CONCURRENT_DOWNLOADS}")
    logger.info("- Smart error codes with retry suggestions")
    logger.info("- Geo-bypass with multiple countries")
    logger.info("- Random delays to avoid pattern detection")
    logger.info("- Enhanced proxy handling with failure recovery")
    logger.info("")
    logger.info("🚨 Rate Limiting Info:")
    logger.info("- YouTube may block server IPs temporarily")
    logger.info("- Longer waits required when rate limited")
    logger.info("- Server uses delays and stealth measures")
    logger.info("- Some videos may be geo-blocked or restricted")
    logger.info("")
    
    if current_proxy:
        logger.info(f"🌐 Proxy: {current_proxy}")
    else:
        logger.info("🌐 Direct connection (no proxy)")
    
    if os.environ.get('RENDER'):
        logger.info("🔥 Running on Render.com")
    
    logger.info("📋 Test with: curl -X POST http://localhost:8080/download/audio/fast -H 'Content-Type: application/json' -d '{\"url\":\"YOUR_YOUTUBE_URL\"}'")
    logger.info("")
    
    # Run with improved settings
    app.run(
        host='0.0.0.0', 
        port=port, 
        debug=False, 
        threaded=True,
        use_reloader=False  # Disable reloader for production
    )
