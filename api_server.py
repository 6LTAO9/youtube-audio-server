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
    handlers=[logging.StreamHandler()]  # Console only for Render
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Render.com free tier optimizations
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300  # Cache files for 5 minutes

# Global variables for tracking
download_status = {}
download_files = {}
active_downloads = 0
MAX_CONCURRENT_DOWNLOADS = 2  # Limit for free tier

# Rate limiting storage (in-memory for free tier)
rate_limit_storage = {}

# Simple proxy management
current_proxy = None
proxy_last_fetched = 0
PROXY_UPDATE_INTERVAL = 3600  # 1 hour

def get_working_proxy():
    """Fetch a working proxy from GitHub - simple implementation"""
    global current_proxy, proxy_last_fetched
    
    # Check if we need to update proxy (every hour or if no proxy)
    current_time = time.time()
    if (current_time - proxy_last_fetched > PROXY_UPDATE_INTERVAL) or (current_proxy is None):
        try:
            # Fetch from TheSpeedX/PROXY-List
            proxy_urls = [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt'
            ]
            
            all_proxies = []
            for url in proxy_urls:
                try:
                    response = requests.get(url, timeout=15)
                    if response.status_code == 200:
                        proxies = [line.strip() for line in response.text.strip().split('\n') if ':' in line.strip()]
                        all_proxies.extend(proxies)
                except Exception as e:
                    logger.warning(f"Failed to fetch from {url}: {e}")
                    continue
            
            if all_proxies:
                # Pick a random proxy and format it
                selected = random.choice(all_proxies)
                current_proxy = f"http://{selected}"
                proxy_last_fetched = current_time
                logger.info(f"Updated proxy: {current_proxy}")
            else:
                logger.warning("No proxies fetched, using direct connection")
                current_proxy = None
                
        except Exception as e:
            logger.error(f"Error fetching proxy: {e}")
            current_proxy = None
    
    return current_proxy

def rate_limit(max_requests=10, window=300):  # 10 requests per 5 minutes
    """Simple in-memory rate limiting"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            now = time.time()
            
            # Clean old entries
            if client_ip in rate_limit_storage:
                rate_limit_storage[client_ip] = [
                    timestamp for timestamp in rate_limit_storage[client_ip]
                    if now - timestamp < window
                ]
            else:
                rate_limit_storage[client_ip] = []
            
            # Check rate limit
            if len(rate_limit_storage[client_ip]) >= max_requests:
                return jsonify({
                    "error": "Rate limit exceeded. Try again later.",
                    "retry_after": window
                }), 429
            
            # Add current request
            rate_limit_storage[client_ip].append(now)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def check_memory_usage():
    """Check memory usage and trigger cleanup if needed"""
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 80:  # If memory usage > 80%
            logger.warning(f"High memory usage: {memory_percent}%")
            cleanup_old_downloads(force=True)
            gc.collect()  # Force garbage collection
            return True
    except Exception as e:
        logger.error(f"Memory check failed: {e}")
    return False

def cleanup_old_downloads(force=False, max_age=1800):  # 30 minutes default
    """Aggressive cleanup for free tier"""
    try:
        current_time = time.time()
        to_delete = []
        
        for job_id, status_info in download_status.items():
            # More aggressive cleanup on free tier
            age_limit = 300 if force else max_age  # 5 min if forced, 30 min otherwise
            
            if current_time - status_info.get('created_at', current_time) > age_limit:
                to_delete.append(job_id)
        
        for job_id in to_delete:
            cleanup_download(job_id, silent=True)
        
        if to_delete:
            logger.info(f"Cleaned up {len(to_delete)} old downloads")
            
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def check_system_resources():
    """Check if system has enough resources for download"""
    global active_downloads
    
    if active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        return False, "Too many concurrent downloads. Try again later."
    
    # Check memory
    if check_memory_usage():
        return False, "Server is under high load. Try again later."
    
    # Check disk space (free tier has limited disk)
    try:
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        if free_gb < 0.5:  # Less than 500MB free
            cleanup_old_downloads(force=True)
            disk_usage = psutil.disk_usage('/tmp')
            free_gb = disk_usage.free / (1024**3)
            if free_gb < 0.2:  # Still less than 200MB
                return False, "Insufficient disk space. Try again later."
    except Exception as e:
        logger.error(f"Disk check failed: {e}")
    
    return True, "OK"

@app.route('/', methods=['GET'])
def health_check():
    """Enhanced health check with system status"""
    try:
        memory_percent = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        
        status = {
            "status": "healthy",
            "service": "youtube-audio-downloader",
            "active_downloads": active_downloads,
            "memory_usage": f"{memory_percent:.1f}%",
            "free_disk_gb": f"{free_gb:.2f}",
            "total_jobs": len(download_status),
            "proxy_status": "active" if current_proxy else "none",
            "proxy_last_updated": time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(proxy_last_fetched)) if proxy_last_fetched else "never"
        }
        
        if memory_percent > 90 or free_gb < 0.1:
            status["status"] = "degraded"
            
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "healthy", "service": "youtube-audio-downloader"}), 200

@app.route('/download/audio/ultrafast', methods=['POST'])
@rate_limit(max_requests=5, window=300)  # Stricter rate limiting
def download_audio_ultrafast():
    """Ultra-fast download optimized for free tier"""
    global active_downloads
    
    # Check resources first
    can_proceed, message = check_system_resources()
    if not can_proceed:
        return jsonify({"error": message}), 503
    
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # URL cleaning
    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    logger.info(f"Ultra-fast download for URL: {youtube_url}")
    
    active_downloads += 1
    temp_dir = None
    
    try:
        # Aggressive cleanup before starting
        cleanup_old_downloads(force=True)
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_')
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        
        # Get proxy automatically
        proxy_url = get_working_proxy()

        # Ultra-optimized options for free tier
        ydl_opts = {
            # Most aggressive format selection for speed
            'format': 'worstaudio[abr>=96]/bestaudio[abr<=128]/bestaudio[ext=m4a][abr<=128]',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '128',  # Lower quality for speed on free tier
            }],
            'postprocessor_args': [
                '-ar', '22050',           # Lower sample rate for speed
                '-ac', '1',               # Mono for smaller size
                '-b:a', '128k',
                '-threads', '1',          # Limited threads on free tier
                '-preset', 'ultrafast',
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            
            # Maximum speed settings for free tier
            'concurrent_fragment_downloads': 2,  # Reduced for free tier
            'http_chunk_size': 512000,           # Smaller chunks
            'buffer_size': 8192,                 # Smaller buffer
            'no_color': True,
            'quiet': True,
            'no_warnings': True,
            
            # Minimal retry for maximum speed
            'socket_timeout': 10,                # Shorter timeout
            'fragment_retries': 0,
            'retries': 0,
            'extractor_retries': 0,
            
            # Skip all unnecessary operations
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
            'extract_flat': False,
            'no_check_certificate': True,        # Skip cert verification for speed
        }

        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
            
        # Add proxy if available
        if proxy_url:
            ydl_opts['proxy'] = proxy_url
            logger.info(f"Using proxy: {proxy_url}")
        else:
            logger.info("Using direct connection (no proxy)")

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])
                
                # Find and return file immediately
                for pattern in ['*.mp3', '*.m4a', '*.webm']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        safe_filename = f"audio_ultrafast_{int(time.time())}.mp3"
                        
                        def cleanup_after_send():
                            """Cleanup after file is sent"""
                            time.sleep(30)  # Wait 30 seconds
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except Exception as e:
                                logger.error(f"Cleanup error: {e}")
                        
                        # Schedule cleanup
                        cleanup_thread = threading.Thread(target=cleanup_after_send)
                        cleanup_thread.daemon = True
                        cleanup_thread.start()
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                
                return jsonify({"error": "Download completed but no audio file was created"}), 500
        
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({"error": "Rate limited. Try again in a few minutes."}), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted"]):
                return jsonify({"error": "Video is unavailable or private"}), 400
            else:
                return jsonify({"error": f"Download failed: {str(e)}"}), 500
    
    finally:
        active_downloads -= 1
        # Cleanup temp directory if still exists
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")

@app.route('/download/audio/async', methods=['POST'])
@rate_limit(max_requests=3, window=600)  # Very strict for async
def download_audio_async():
    """Async download with better resource management"""
    can_proceed, message = check_system_resources()
    if not can_proceed:
        return jsonify({"error": message}), 503
    
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # Cleanup before starting new download
    cleanup_old_downloads()

    job_id = str(uuid.uuid4())
    
    download_status[job_id] = {
        'status': 'started',
        'progress': 0,
        'message': 'Download queued',
        'created_at': time.time()
    }
    
    thread = threading.Thread(target=background_download_optimized, args=(job_id, youtube_url))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "started",
        "check_url": f"/download/status/{job_id}",
        "download_url": f"/download/file/{job_id}",
        "note": "Files auto-delete after 30 minutes on free tier"
    }), 202

def background_download_optimized(job_id, youtube_url):
    """Optimized background download for free tier"""
    global active_downloads
    active_downloads += 1
    temp_dir = None
    
    try:
        # Update status
        download_status[job_id].update({
            'status': 'processing',
            'progress': 10,
            'message': 'Processing video URL'
        })
        
        # Clean URL
        if "&list=" in youtube_url:
            youtube_url = youtube_url.split("&list=")[0]
        
        # Check memory before proceeding
        if check_memory_usage():
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'Server overloaded. Try again later.'
            })
            return
        
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_async_')
        
        download_status[job_id].update({
            'progress': 25,
            'message': 'Downloading audio...'
        })
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        
        # Get proxy automatically
        proxy_url = get_working_proxy()
        
        # Free tier optimized settings
        ydl_opts = {
            'format': 'bestaudio[abr<=192]/bestaudio[ext=m4a][abr<=192]/bestaudio',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',  # Good quality but not excessive
            }],
            'postprocessor_args': [
                '-ar', '44100',
                '-ac', '2',
                '-b:a', '192k',
                '-threads', '2',              # Limited threads
                '-preset', 'fast',            # Balanced preset
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            
            # Free tier network settings
            'concurrent_fragment_downloads': 2,
            'http_chunk_size': 1048576,
            'buffer_size': 16384,
            'no_color': True,
            'quiet': False,
            
            # Conservative timeouts for free tier
            'socket_timeout': 30,
            'fragment_retries': 1,
            'retries': 1,
            'extractor_retries': 1,
            
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
        }
        
        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
            
        # Add proxy if available
        if proxy_url:
            ydl_opts['proxy'] = proxy_url
            logger.info(f"Async download using proxy: {proxy_url}")
        
        download_status[job_id].update({
            'progress': 50,
            'message': 'Converting to MP3...'
        })
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([youtube_url])
            
            # Find downloaded file
            for pattern in ['*.mp3', '*.m4a', '*.webm']:
                found_files = list(Path(temp_dir).glob(pattern))
                if found_files:
                    file_path = str(found_files[0])
                    download_files[job_id] = file_path
                    
                    download_status[job_id].update({
                        'status': 'completed',
                        'progress': 100,
                        'message': 'Download completed successfully'
                    })
                    
                    # Shorter cleanup time for free tier
                    cleanup_timer = threading.Timer(1800, cleanup_download, args=(job_id,))
                    cleanup_timer.daemon = True
                    cleanup_timer.start()
                    return
            
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'No audio file was created'
            })
    
    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            message = "Rate limited by YouTube. Try again later."
        elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted"]):
            message = "Video is unavailable or private"
        else:
            message = f"Download failed: {str(e)}"
        
        download_status[job_id].update({
            'status': 'failed',
            'progress': 0,
            'message': message
        })
    
    finally:
        active_downloads -= 1
        # Cleanup temp directory on failure
        if temp_dir and job_id not in download_files:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Background cleanup error: {e}")

@app.route('/download/status/<job_id>', methods=['GET'])
def check_download_status(job_id):
    """Check download status with auto-cleanup"""
    if job_id not in download_status:
        return jsonify({"error": "Job not found or expired"}), 404
    
    status = download_status[job_id].copy()
    
    # Add time remaining info
    if 'created_at' in status:
        age = time.time() - status['created_at']
        remaining = max(0, 1800 - age)  # 30 minutes total
        status['expires_in_seconds'] = int(remaining)
    
    return jsonify(status)

@app.route('/download/file/<job_id>', methods=['GET'])
def get_download_file(job_id):
    """Get downloaded file with automatic cleanup"""
    if job_id not in download_status:
        return jsonify({"error": "Job not found or expired"}), 404
    
    status = download_status[job_id]
    if status['status'] != 'completed':
        return jsonify({"error": "Download not ready", "status": status['status']}), 400
    
    if job_id not in download_files:
        return jsonify({"error": "File expired or not found"}), 404
    
    file_path = download_files[job_id]
    if not os.path.exists(file_path):
        cleanup_download(job_id, silent=True)
        return jsonify({"error": "File no longer available"}), 404
    
    # Schedule cleanup after download
    def delayed_cleanup():
        time.sleep(60)  # Wait 1 minute
        cleanup_download(job_id, silent=True)
    
    cleanup_thread = threading.Thread(target=delayed_cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    
    return send_file(
        file_path,
        as_attachment=True,
        download_name=f"audio_hq_{job_id[:8]}.mp3",
        mimetype='audio/mpeg'
    )

def cleanup_download(job_id, silent=False):
    """Clean up download files and status"""
    try:
        if job_id in download_files:
            file_path = download_files[job_id]
            if os.path.exists(file_path):
                # Remove file and parent temp directory
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

# Keep original endpoint for compatibility
@app.route('/download/audio', methods=['POST'])
@rate_limit()
def download_audio():
    """Original endpoint - redirects to ultrafast for free tier"""
    return download_audio_ultrafast()

@app.route('/download/audio/fast', methods=['POST'])
@rate_limit()
def download_audio_fast():
    """Fast download endpoint - optimized for free tier"""
    return download_audio_ultrafast()  # Use ultrafast on free tier

# Disabled endpoint for free tier
@app.route('/download/audio/lossless', methods=['POST'])
def download_audio_lossless():
    """Lossless downloads - not available on free tier"""
    return jsonify({
        "error": "Lossless downloads not available on free tier",
        "suggestion": "Use /download/audio/ultrafast for fastest downloads"
    }), 403

def periodic_cleanup():
    """Run periodic cleanup every 10 minutes"""
    while True:
        try:
            time.sleep(600)  # 10 minutes
            cleanup_old_downloads()
            gc.collect()
            logger.info("Periodic cleanup completed")
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {e}")
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(413)
def handle_file_too_large(e):
    return jsonify({
        "error": "File too large for free tier",
        "limit": "100MB maximum"
    }), 413

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    
    # Free tier optimizations
    os.environ['FFMPEG_THREADS'] = '2'      # Limited threads
    os.environ['MALLOC_ARENA_MAX'] = '1'    # Minimize memory usage
    
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    # Initialize proxy on startup
    logger.info("Fetching initial proxy list...")
    try:
        get_working_proxy()
    except Exception as e:
        logger.warning(f"Initial proxy fetch failed: {e}")
    
    # System checks
    try:
        import subprocess
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True, text=True)
        logger.info("FFmpeg is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FFmpeg not found - downloads may fail")
    
    try:
        logger.info(f"yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.warning("Could not determine yt-dlp version")
    
    logger.info("=== YouTube Audio Downloader Server ===")
    logger.info("Optimized for Render.com FREE TIER with AUTO-PROXY")
    logger.info("Available endpoints:")
    logger.info("- POST /download/audio/ultrafast (128kbps, maximum speed)")
    logger.info("- POST /download/audio/async (192kbps, no timeout)")
    logger.info("- GET  /download/status/{job_id} (check async status)")
    logger.info("- GET  /download/file/{job_id} (get async file)")
    logger.info("- GET  / (health check)")
    logger.info("Features:")
    logger.info("- Auto-proxy from TheSpeedX/PROXY-List")
    logger.info("- Rate limiting active (prevents abuse)")
    logger.info("- Auto-cleanup every 10 minutes")
    logger.info("- Files expire after 30 minutes")
    logger.info("- Max 2 concurrent downloads")
    logger.info("- Memory and disk monitoring")
    
    if current_proxy:
        logger.info(f"- Proxy active: {current_proxy}")
    else:
        logger.info("- No proxy (direct connection)")
    
    if os.environ.get('RENDER'):
        logger.info("🚀 Running on Render.com with optimizations enabled")
    
    # Run with threading enabled for better performance
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
        if new_proxies:
            # Remove duplicates and shuffle
            unique_proxies = list(set(new_proxies))
            random.shuffle(unique_proxies)
            
            with proxy_lock:
                proxy_list = unique_proxies
                proxy_last_updated = time.time()
            
            logger.info(f"Updated proxy list with {len(unique_proxies)} unique proxies")
            return True
        else:
            logger.warning("No proxies were fetched from any source")
            return False
            
    except Exception as e:
        logger.error(f"Critical error fetching proxies: {e}")
        return False

def test_proxy(proxy, timeout=10):
    """Test if a proxy is working"""
    try:
        test_urls = [
            'http://httpbin.org/ip',
            'https://api.ipify.org?format=json',
            'http://ip-api.com/json'
        ]
        
        proxies = {'http': proxy, 'https': proxy}
        
        for test_url in test_urls:
            try:
                response = requests.get(test_url, proxies=proxies, timeout=timeout)
                if response.status_code == 200:
                    return True
            except:
                continue
        
        return False
    except Exception:
        return False

def get_working_proxy():
    """Get a working proxy from the list"""
    global current_proxy_index, proxy_list
    
    # Check if we need to update proxy list
    if time.time() - proxy_last_updated > proxy_update_interval:
        threading.Thread(target=fetch_proxies_from_github, daemon=True).start()
    
    if not proxy_list:
        logger.warning("No proxies available")
        return None
    
    with proxy_lock:
        # Try up to 5 proxies before giving up
        for _ in range(min(5, len(proxy_list))):
            current_proxy_index = (current_proxy_index + 1) % len(proxy_list)
            proxy = proxy_list[current_proxy_index]
            
            # Quick test (shorter timeout for performance)
            if test_proxy(proxy, timeout=5):
                logger.info(f"Using working proxy: {proxy}")
                return proxy
            else:
                logger.debug(f"Proxy not working: {proxy}")
        
        logger.warning("No working proxies found in current batch")
        return None

def update_proxies_periodic():
    """Periodically update proxy list"""
    while True:
        try:
            time.sleep(proxy_update_interval)
            logger.info("Starting periodic proxy update...")
            fetch_proxies_from_github()
        except Exception as e:
            logger.error(f"Error in periodic proxy update: {e}")

def rate_limit(max_requests=10, window=300):  # 10 requests per 5 minutes
    """Simple in-memory rate limiting"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            now = time.time()
            
            # Clean old entries
            if client_ip in rate_limit_storage:
                rate_limit_storage[client_ip] = [
                    timestamp for timestamp in rate_limit_storage[client_ip]
                    if now - timestamp < window
                ]
            else:
                rate_limit_storage[client_ip] = []
            
            # Check rate limit
            if len(rate_limit_storage[client_ip]) >= max_requests:
                return jsonify({
                    "error": "Rate limit exceeded. Try again later.",
                    "retry_after": window
                }), 429
            
            # Add current request
            rate_limit_storage[client_ip].append(now)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def check_memory_usage():
    """Check memory usage and trigger cleanup if needed"""
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 80:  # If memory usage > 80%
            logger.warning(f"High memory usage: {memory_percent}%")
            cleanup_old_downloads(force=True)
            gc.collect()  # Force garbage collection
            return True
    except Exception as e:
        logger.error(f"Memory check failed: {e}")
    return False

def cleanup_old_downloads(force=False, max_age=1800):  # 30 minutes default
    """Aggressive cleanup for free tier"""
    try:
        current_time = time.time()
        to_delete = []
        
        for job_id, status_info in download_status.items():
            # More aggressive cleanup on free tier
            age_limit = 300 if force else max_age  # 5 min if forced, 30 min otherwise
            
            if current_time - status_info.get('created_at', current_time) > age_limit:
                to_delete.append(job_id)
        
        for job_id in to_delete:
            cleanup_download(job_id, silent=True)
        
        if to_delete:
            logger.info(f"Cleaned up {len(to_delete)} old downloads")
            
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def check_system_resources():
    """Check if system has enough resources for download"""
    global active_downloads
    
    if active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        return False, "Too many concurrent downloads. Try again later."
    
    # Check memory
    if check_memory_usage():
        return False, "Server is under high load. Try again later."
    
    # Check disk space (free tier has limited disk)
    try:
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        if free_gb < 0.5:  # Less than 500MB free
            cleanup_old_downloads(force=True)
            disk_usage = psutil.disk_usage('/tmp')
            free_gb = disk_usage.free / (1024**3)
            if free_gb < 0.2:  # Still less than 200MB
                return False, "Insufficient disk space. Try again later."
    except Exception as e:
        logger.error(f"Disk check failed: {e}")
    
    return True, "OK"

@app.route('/', methods=['GET'])
def health_check():
    """Enhanced health check with system status"""
    try:
        memory_percent = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        
        status = {
            "status": "healthy",
            "service": "youtube-audio-downloader",
            "active_downloads": active_downloads,
            "memory_usage": f"{memory_percent:.1f}%",
            "free_disk_gb": f"{free_gb:.2f}",
            "total_jobs": len(download_status),
            "proxy_count": len(proxy_list),
            "proxy_last_updated": time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(proxy_last_updated)) if proxy_last_updated else "Never"
        }
        
        if memory_percent > 90 or free_gb < 0.1:
            status["status"] = "degraded"
            
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "healthy", "service": "youtube-audio-downloader"}), 200

@app.route('/proxies/refresh', methods=['POST'])
def refresh_proxies():
    """Manual proxy refresh endpoint"""
    success = fetch_proxies_from_github()
    if success:
        return jsonify({
            "status": "success",
            "proxy_count": len(proxy_list),
            "message": "Proxy list updated successfully"
        })
    else:
        return jsonify({
            "status": "error",
            "message": "Failed to update proxy list"
        }), 500

def create_ydl_opts_with_proxy(temp_dir, quality='ultrafast'):
    """Create yt-dlp options with automatic proxy selection"""
    # Get working proxy
    proxy_url = get_working_proxy()
    
    # Base configuration based on quality
    if quality == 'ultrafast':
        ydl_opts = {
            'format': 'worstaudio[abr>=96]/bestaudio[abr<=128]/bestaudio[ext=m4a][abr<=128]',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '128',
            }],
            'postprocessor_args': [
                '-ar', '22050',
                '-ac', '1',
                '-b:a', '128k',
                '-threads', '1',
                '-preset', 'ultrafast',
            ],
        }
    else:  # async/high quality
        ydl_opts = {
            'format': 'bestaudio[abr<=192]/bestaudio[ext=m4a][abr<=192]/bestaudio',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'postprocessor_args': [
                '-ar', '44100',
                '-ac', '2',
                '-b:a', '192k',
                '-threads', '2',
                '-preset', 'fast',
            ],
        }
    
    # Common options
    ydl_opts.update({
        'prefer_ffmpeg': True,
        'keepvideo': False,
        'noplaylist': True,
        'concurrent_fragment_downloads': 2,
        'http_chunk_size': 512000,
        'buffer_size': 8192,
        'no_color': True,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 15,
        'fragment_retries': 1,
        'retries': 2,
        'extractor_retries': 1,
        'writesubtitles': False,
        'writeautomaticsub': False,
        'embed_subs': False,
        'writeinfojson': False,
        'writethumbnail': False,
        'extract_flat': False,
        'no_check_certificate': True,
    })
    
    # Add proxy if available
    if proxy_url:
        ydl_opts['proxy'] = proxy_url
        logger.info(f"Using proxy for download: {proxy_url}")
    else:
        logger.info("No proxy available, using direct connection")
    
    # Add cookies if available
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(script_dir, 'cookies.txt')
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path
    
    return ydl_opts

@app.route('/download/audio/ultrafast', methods=['POST'])
@rate_limit(max_requests=5, window=300)
def download_audio_ultrafast():
    """Ultra-fast download with automatic proxy selection"""
    global active_downloads
    
    # Check resources first
    can_proceed, message = check_system_resources()
    if not can_proceed:
        return jsonify({"error": message}), 503
    
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # URL cleaning
    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    logger.info(f"Ultra-fast download for URL: {youtube_url}")
    
    active_downloads += 1
    temp_dir = None
    
    try:
        # Aggressive cleanup before starting
        cleanup_old_downloads(force=True)
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_')
        
        # Create yt-dlp options with automatic proxy
        ydl_opts = create_ydl_opts_with_proxy(temp_dir, 'ultrafast')

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])
                
                # Find and return file immediately
                for pattern in ['*.mp3', '*.m4a', '*.webm']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        safe_filename = f"audio_ultrafast_{int(time.time())}.mp3"
                        
                        def cleanup_after_send():
                            """Cleanup after file is sent"""
                            time.sleep(30)  # Wait 30 seconds
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except Exception as e:
                                logger.error(f"Cleanup error: {e}")
                        
                        # Schedule cleanup
                        cleanup_thread = threading.Thread(target=cleanup_after_send)
                        cleanup_thread.daemon = True
                        cleanup_thread.start()
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                
                return jsonify({"error": "Download completed but no audio file was created"}), 500
        
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({"error": "Rate limited. Try again in a few minutes."}), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted"]):
                return jsonify({"error": "Video is unavailable or private"}), 400
            else:
                return jsonify({"error": f"Download failed: {str(e)}"}), 500
    
    finally:
        active_downloads -= 1
        # Cleanup temp directory if still exists
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")

@app.route('/download/audio/async', methods=['POST'])
@rate_limit(max_requests=3, window=600)
def download_audio_async():
    """Async download with automatic proxy selection"""
    can_proceed, message = check_system_resources()
    if not can_proceed:
        return jsonify({"error": message}), 503
    
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # Cleanup before starting new download
    cleanup_old_downloads()

    job_id = str(uuid.uuid4())
    
    download_status[job_id] = {
        'status': 'started',
        'progress': 0,
        'message': 'Download queued',
        'created_at': time.time()
    }
    
    thread = threading.Thread(target=background_download_optimized, args=(job_id, youtube_url))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "started",
        "check_url": f"/download/status/{job_id}",
        "download_url": f"/download/file/{job_id}",
        "note": "Files auto-delete after 30 minutes on free tier"
    }), 202

def background_download_optimized(job_id, youtube_url):
    """Optimized background download with automatic proxy"""
    global active_downloads
    active_downloads += 1
    temp_dir = None
    
    try:
        # Update status
        download_status[job_id].update({
            'status': 'processing',
            'progress': 10,
            'message': 'Processing video URL'
        })
        
        # Clean URL
        if "&list=" in youtube_url:
            youtube_url = youtube_url.split("&list=")[0]
        
        # Check memory before proceeding
        if check_memory_usage():
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'Server overloaded. Try again later.'
            })
            return
        
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_async_')
        
        download_status[job_id].update({
            'progress': 25,
            'message': 'Downloading audio...'
        })
        
        # Create yt-dlp options with automatic proxy
        ydl_opts = create_ydl_opts_with_proxy(temp_dir, 'async')
        
        download_status[job_id].update({
            'progress': 50,
            'message': 'Converting to MP3...'
        })
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([youtube_url])
            
            # Find downloaded file
            for pattern in ['*.mp3', '*.m4a', '*.webm']:
                found_files = list(Path(temp_dir).glob(pattern))
                if found_files:
                    file_path = str(found_files[0])
                    download_files[job_id] = file_path
                    
                    download_status[job_id].update({
                        'status': 'completed',
                        'progress': 100,
                        'message': 'Download completed successfully'
                    })
                    
                    # Shorter cleanup time for free tier
                    cleanup_timer = threading.Timer(1800, cleanup_download, args=(job_id,))
                    cleanup_timer.daemon = True
                    cleanup_timer.start()
                    return
            
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'No audio file was created'
            })
    
    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            message = "Rate limited by YouTube. Try again later."
        elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted"]):
            message = "Video is unavailable or private"
        else:
            message = f"Download failed: {str(e)}"
        
        download_status[job_id].update({
            'status': 'failed',
            'progress': 0,
            'message': message
        })
    
    finally:
        active_downloads -= 1
        # Cleanup temp directory on failure
        if temp_dir and job_id not in download_files:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Background cleanup error: {e}")

@app.route('/download/status/<job_id>', methods=['GET'])
def check_download_status(job_id):
    """Check download status with auto-cleanup"""
    if job_id not in download_status:
        return jsonify({"error": "Job not found or expired"}), 404
    
    status = download_status[job_id].copy()
    
    # Add time remaining info
    if 'created_at' in status:
        age = time.time() - status['created_at']
        remaining = max(0, 1800 - age)  # 30 minutes total
        status['expires_in_seconds'] = int(remaining)
    
    return jsonify(status)

@app.route('/download/file/<job_id>', methods=['GET'])
def get_download_file(job_id):
    """Get downloaded file with automatic cleanup"""
    if job_id not in download_status:
        return jsonify({"error": "Job not found or expired"}), 404
    
    status = download_status[job_id]
    if status['status'] != 'completed':
        return jsonify({"error": "Download not ready", "status": status['status']}), 400
    
    if job_id not in download_files:
        return jsonify({"error": "File expired or not found"}), 404
    
    file_path = download_files[job_id]
    if not os.path.exists(file_path):
        cleanup_download(job_id, silent=True)
        return jsonify({"error": "File no longer available"}), 404
    
    # Schedule cleanup after download
    def delayed_cleanup():
        time.sleep(60)  # Wait 1 minute
        cleanup_download(job_id, silent=True)
    
    cleanup_thread = threading.Thread(target=delayed_cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    
    return send_file(
        file_path,
        as_attachment=True,
        download_name=f"audio_hq_{job_id[:8]}.mp3",
        mimetype='audio/mpeg'
    )

def cleanup_download(job_id, silent=False):
    """Clean up download files and status"""
    try:
        if job_id in download_files:
            file_path = download_files[job_id]
            if os.path.exists(file_path):
                # Remove file and parent temp directory
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

# Keep original endpoint for compatibility
@app.route('/download/audio', methods=['POST'])
@rate_limit()
def download_audio():
    """Original endpoint - redirects to ultrafast for free tier"""
    return download_audio_ultrafast()

@app.route('/download/audio/fast', methods=['POST'])
@rate_limit()
def download_audio_fast():
    """Fast download endpoint - optimized for free tier"""
    return download_audio_ultrafast()  # Use ultrafast on free tier

# Disabled endpoint for free tier
@app.route('/download/audio/lossless', methods=['POST'])
def download_audio_lossless():
    """Lossless downloads - not available on free tier"""
    return jsonify({
        "error": "Lossless downloads not available on free tier",
        "suggestion": "Use /download/audio/ultrafast for fastest downloads"
    }), 403

def periodic_cleanup():
    """Run periodic cleanup every 10 minutes"""
    while True:
        try:
            time.sleep(600)  # 10 minutes
            cleanup_old_downloads()
            gc.collect()
            logger.info("Periodic cleanup completed")
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {e}")
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(413)
def handle_file_too_large(e):
    return jsonify({
        "error": "File too large for free tier",
        "limit": "100MB maximum"
    }), 413

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    
    # Free tier optimizations
    os.environ['FFMPEG_THREADS'] = '2'      # Limited threads
    os.environ['MALLOC_ARENA_MAX'] = '1'    # Minimize memory usage
    
    # Initialize proxy list on startup
    logger.info("Initializing proxy list from GitHub...")
    fetch_proxies_from_github()
    
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    # Start proxy update thread
    proxy_thread = threading.Thread(target=update_proxies_periodic, daemon=True)
    proxy_thread.start()
    
    # System checks
    try:
        import subprocess
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True, text=True)
        logger.info("FFmpeg is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FFmpeg not found - downloads may fail")
    
    try:
        logger.info(f"yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.warning("Could not determine yt-dlp version")
    
    logger.info("=== YouTube Audio Downloader Server ===")
    logger.info("Optimized for Render.com FREE TIER with AUTO-PROXY")
    logger.info("Available endpoints:")
    logger.info("- POST /download/audio/ultrafast (128kbps, maximum speed)")
    logger.info("- POST /download/audio/async (192kbps, no timeout)")
    logger.info("- GET  /download/status/{job_id} (check async status)")
    logger.info("- GET  /download/file/{job_id} (get async file)")
    logger.info("- POST /proxies/refresh (manually refresh proxy list)")
    logger.info("- GET  / (health check)")
    logger.info("Features:")
    logger.info("- Auto-proxy from TheSpeedX/PROXY-List")
    logger.info("- Rate limiting active (prevents abuse)")
    logger.info("- Auto-cleanup every 10 minutes")
    logger.info("- Files expire after 30 minutes")
    logger.info("- Max 2 concurrent downloads")
    logger.info("- Memory and disk monitoring")
    logger.info(f"- Proxy count: {len(proxy_list)}")
    
    if os.environ.get('RENDER'):
        logger.info("🚀 Running on Render.com with optimizations enabled")
    
    # Run with threading enabled for better performance
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
