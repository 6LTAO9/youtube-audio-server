import os
import tempfile
import time
import threading
import uuid
import gc
import psutil
import requests
import re
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import yt_dlp
from pathlib import Path
import logging
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300

# Global state
download_status = {}
download_files = {}
active_downloads = 0
MAX_CONCURRENT_DOWNLOADS = 2
rate_limit_storage = {}
proxy_list = []
proxy_index = 0
current_proxy = None
proxy_last_fetched = 0

def validate_youtube_url(url):
    """Validate and clean YouTube URL"""
    if not url or not isinstance(url, str):
        return None, "Invalid URL format"
    
    # Clean URL
    if "&list=" in url:
        url = url.split("&list=")[0]
    if "&t=" in url:
        url = url.split("&t=")[0]
    
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
        r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
    ]
    
    for pattern in patterns:
        if re.search(pattern, url):
            return url, None
    
    return None, "Not a valid YouTube URL"

def get_working_proxy():
    """Get working proxy with rotation"""
    global current_proxy, proxy_last_fetched, proxy_list, proxy_index
    
    # Update proxy list every 30 minutes
    if time.time() - proxy_last_fetched > 1800 or not proxy_list:
        try:
            logger.info("Fetching proxy list...")
            proxy_urls = [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
                'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt'
            ]
            
            all_proxies = []
            for url in proxy_urls:
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        for line in response.text.strip().split('\n'):
                            line = line.strip()
                            if ':' in line and not line.startswith('#'):
                                if line.count(':') == 1:
                                    all_proxies.append(line)
                                else:
                                    parts = line.split(':')
                                    if len(parts) >= 2:
                                        all_proxies.append(f"{parts[0]}:{parts[1]}")
                except Exception:
                    continue
            
            # Validate proxies
            valid_proxies = []
            for proxy in set(all_proxies):  # Remove duplicates
                try:
                    ip, port = proxy.split(':')
                    if (len(ip.split('.')) == 4 and 
                        all(0 <= int(octet) <= 255 for octet in ip.split('.')) and
                        1 <= int(port) <= 65535):
                        valid_proxies.append(proxy)
                except (ValueError, IndexError):
                    continue
            
            proxy_list = valid_proxies
            proxy_index = 0
            proxy_last_fetched = time.time()
            logger.info(f"Updated proxy list: {len(proxy_list)} proxies")
                
        except Exception as e:
            logger.error(f"Proxy update error: {e}")
    
    # Find working proxy
    if proxy_list:
        for _ in range(min(5, len(proxy_list))):
            if proxy_index >= len(proxy_list):
                proxy_index = 0
            
            proxy = f"http://{proxy_list[proxy_index]}"
            if test_proxy(proxy):
                current_proxy = proxy
                return current_proxy
            proxy_index += 1
    
    current_proxy = None
    return current_proxy

def test_proxy(proxy_url, timeout=8):
    """Test proxy connectivity"""
    try:
        proxies = {'http': proxy_url, 'https': proxy_url}
        response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=timeout)
        return response.status_code == 200 and len(response.text.strip()) > 5
    except Exception:
        return False

def rate_limit(max_requests=10, window=300):
    """Rate limiting decorator"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            now = time.time()
            
            # Clean old entries
            if client_ip in rate_limit_storage:
                rate_limit_storage[client_ip] = [
                    t for t in rate_limit_storage[client_ip] if now - t < window
                ]
            else:
                rate_limit_storage[client_ip] = []
            
            # Check rate limit
            if len(rate_limit_storage[client_ip]) >= max_requests:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": "Too many requests. Please wait."
                    }
                }), 429
            
            rate_limit_storage[client_ip].append(now)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def check_system_resources():
    """Check system resources"""
    if active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        return False, "TOO_MANY_DOWNLOADS", "Server busy"
    
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 85:
            cleanup_old_downloads(force=True)
            if psutil.virtual_memory().percent > 90:
                return False, "HIGH_MEMORY_USAGE", "Server overloaded"
        
        free_gb = psutil.disk_usage('/tmp').free / (1024**3)
        if free_gb < 0.2:
            return False, "LOW_DISK_SPACE", "Insufficient disk space"
        
        return True, "OK", "Resources available"
    except Exception:
        return True, "OK", "Resource check bypassed"

def cleanup_old_downloads(force=False, max_age=1800):
    """Clean up old downloads"""
    try:
        current_time = time.time()
        to_delete = []
        
        for job_id, status_info in download_status.items():
            age_limit = 300 if force else max_age
            if current_time - status_info.get('created_at', current_time) > age_limit:
                to_delete.append(job_id)
        
        for job_id in to_delete:
            cleanup_download(job_id)
        
        if to_delete:
            logger.info(f"Cleaned up {len(to_delete)} old downloads")
            
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def cleanup_download(job_id):
    """Clean up specific download"""
    try:
        if job_id in download_files:
            file_path = download_files[job_id]
            if os.path.exists(file_path):
                import shutil
                shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
            del download_files[job_id]
        
        if job_id in download_status:
            del download_status[job_id]
            
    except Exception as e:
        logger.error(f"Cleanup error for {job_id}: {e}")

def get_ydl_opts(temp_dir, quality='128'):
    """Get yt-dlp options"""
    opts = {
        'format': 'bestaudio[abr<=192]/bestaudio' if quality == '192' else 'worstaudio[abr>=64]/bestaudio[abr<=128]',
        'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': quality,
        }],
        'postprocessor_args': [
            '-ar', '44100' if quality == '192' else '22050',
            '-ac', '2' if quality == '192' else '1',
            '-b:a', f'{quality}k',
            '-threads', '2',
        ],
        'prefer_ffmpeg': True,
        'keepvideo': False,
        'noplaylist': True,
        'no_color': True,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 30,
        'retries': 2,
        'writesubtitles': False,
        'writeinfojson': False,
        'writethumbnail': False,
    }
    
    if current_proxy:
        opts['proxy'] = current_proxy
    
    return opts

def handle_download_error(e):
    """Handle download errors and return appropriate error info"""
    error_str = str(e).lower()
    
    if "429" in error_str or "too many requests" in error_str:
        return "YOUTUBE_RATE_LIMIT", "YouTube rate limit. Try again later."
    elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
        return "VIDEO_UNAVAILABLE", "Video unavailable or removed"
    elif "network" in error_str or "connection" in error_str or "timeout" in error_str:
        return "NETWORK_ERROR", "Network connection problem"
    else:
        return "DOWNLOAD_ERROR", str(e)

@app.before_request
def log_request_info():
    logger.info(f"Request: {request.method} {request.url}")

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        memory_percent = psutil.virtual_memory().percent
        free_gb = psutil.disk_usage('/tmp').free / (1024**3)
        
        return jsonify({
            "success": True,
            "data": {
                "service": "youtube-audio-downloader",
                "status": "healthy",
                "active_downloads": active_downloads,
                "memory_usage": f"{memory_percent:.1f}%",
                "free_disk_gb": f"{free_gb:.2f}",
                "proxy_status": "active" if current_proxy else "direct"
            }
        })
    except Exception:
        return jsonify({
            "success": True,
            "data": {"service": "youtube-audio-downloader", "status": "healthy"}
        })

@app.route('/download/audio/ultrafast', methods=['POST'])
@rate_limit(max_requests=8, window=300)
def download_audio_ultrafast():
    """Ultra-fast download endpoint"""
    global active_downloads
    
    can_proceed, error_code, error_message = check_system_resources()
    if not can_proceed:
        return jsonify({
            "success": False,
            "error": {"code": error_code, "message": error_message}
        }), 503
    
    if not request.is_json:
        return jsonify({
            "success": False,
            "error": {"code": "INVALID_REQUEST", "message": "Request must be JSON"}
        }), 400
    
    try:
        data = request.json
        youtube_url = data.get('url')
        
        if not youtube_url:
            return jsonify({
                "success": False,
                "error": {"code": "MISSING_URL", "message": "URL required"}
            }), 400
        
        clean_url, url_error = validate_youtube_url(youtube_url)
        if url_error:
            return jsonify({
                "success": False,
                "error": {"code": "INVALID_URL", "message": url_error}
            }), 400
        
        active_downloads += 1
        temp_dir = None
        
        try:
            cleanup_old_downloads(force=True)
            temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_')
            get_working_proxy()  # Update proxy
            
            ydl_opts = get_ydl_opts(temp_dir, '128')
            
            # Try download
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([clean_url])
            except Exception as e:
                # Retry without proxy if proxy failed
                if 'proxy' in ydl_opts and 'proxy' in str(e).lower():
                    del ydl_opts['proxy']
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([clean_url])
                else:
                    raise
            
            # Find and return file
            for pattern in ['*.mp3', '*.m4a', '*.webm']:
                found_files = list(Path(temp_dir).glob(pattern))
                if found_files:
                    file_path = str(found_files[0])
                    file_size = os.path.getsize(file_path)
                    safe_filename = f"audio_{int(time.time())}.mp3"
                    
                    # Schedule cleanup
                    def cleanup_later():
                        time.sleep(60)
                        try:
                            import shutil
                            shutil.rmtree(temp_dir, ignore_errors=True)
                        except Exception:
                            pass
                    
                    threading.Thread(target=cleanup_later, daemon=True).start()
                    
                    response = send_file(
                        file_path,
                        as_attachment=True,
                        download_name=safe_filename,
                        mimetype='audio/mpeg'
                    )
                    
                    response.headers['X-File-Size'] = str(file_size)
                    return response
            
            return jsonify({
                "success": False,
                "error": {"code": "NO_AUDIO_FILE", "message": "No audio file created"}
            }), 500
        
        finally:
            active_downloads -= 1
            if temp_dir and os.path.exists(temp_dir):
                try:
                    import shutil
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception:
                    pass
    
    except Exception as e:
        error_code, error_message = handle_download_error(e)
        return jsonify({
            "success": False,
            "error": {"code": error_code, "message": error_message}
        }), 500

@app.route('/download/audio/async', methods=['POST'])
@rate_limit(max_requests=5, window=600)
def download_audio_async():
    """Async download endpoint"""
    can_proceed, error_code, error_message = check_system_resources()
    if not can_proceed:
        return jsonify({
            "success": False,
            "error": {"code": error_code, "message": error_message}
        }), 503
    
    if not request.is_json:
        return jsonify({
            "success": False,
            "error": {"code": "INVALID_REQUEST", "message": "Request must be JSON"}
        }), 400
    
    try:
        data = request.json
        youtube_url = data.get('url')
        
        if not youtube_url:
            return jsonify({
                "success": False,
                "error": {"code": "MISSING_URL", "message": "URL required"}
            }), 400
        
        clean_url, url_error = validate_youtube_url(youtube_url)
        if url_error:
            return jsonify({
                "success": False,
                "error": {"code": "INVALID_URL", "message": url_error}
            }), 400
        
        cleanup_old_downloads()
        job_id = str(uuid.uuid4())
        
        download_status[job_id] = {
            'status': 'queued',
            'progress': 0,
            'message': 'Download queued',
            'created_at': time.time(),
            'url': clean_url
        }
        
        thread = threading.Thread(target=background_download, args=(job_id, clean_url))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            "success": True,
            "data": {
                "job_id": job_id,
                "status": "queued",
                "check_url": f"/download/status/{job_id}",
                "download_url": f"/download/file/{job_id}"
            }
        }), 202
    
    except Exception:
        return jsonify({
            "success": False,
            "error": {"code": "INTERNAL_ERROR", "message": "Failed to start download"}
        }), 500

def background_download(job_id, youtube_url):
    """Background download worker"""
    global active_downloads
    active_downloads += 1
    
    try:
        download_status[job_id].update({
            'status': 'processing',
            'progress': 25,
            'message': 'Downloading audio...'
        })
        
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_async_')
        get_working_proxy()  # Update proxy
        
        ydl_opts = get_ydl_opts(temp_dir, '192')
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])
                
                # Find downloaded file
                for pattern in ['*.mp3', '*.m4a', '*.webm']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        file_size = os.path.getsize(file_path)
                        download_files[job_id] = file_path
                        
                        download_status[job_id].update({
                            'status': 'completed',
                            'progress': 100,
                            'message': 'Download completed',
                            'file_size': file_size
                        })
                        
                        # Auto-cleanup after 30 minutes
                        cleanup_timer = threading.Timer(1800, cleanup_download, args=(job_id,))
                        cleanup_timer.daemon = True
                        cleanup_timer.start()
                        return
                
                download_status[job_id].update({
                    'status': 'failed',
                    'message': 'No audio file created',
                    'error_code': 'NO_AUDIO_FILE'
                })
        
        except Exception as e:
            error_code, message = handle_download_error(e)
            download_status[job_id].update({
                'status': 'failed',
                'message': message,
                'error_code': error_code
            })
    
    except Exception:
        download_status[job_id].update({
            'status': 'failed',
            'message': 'Unexpected error',
            'error_code': 'INTERNAL_ERROR'
        })
    
    finally:
        active_downloads -= 1

@app.route('/download/status/<job_id>', methods=['GET'])
def check_download_status(job_id):
    """Check download status"""
    if job_id not in download_status:
        return jsonify({
            "success": False,
            "error": {"code": "JOB_NOT_FOUND", "message": "Job not found"}
        }), 404
    
    return jsonify({"success": True, "data": download_status[job_id]})

@app.route('/download/file/<job_id>', methods=['GET'])
def get_download_file(job_id):
    """Get download file"""
    if job_id not in download_status:
        return jsonify({
            "success": False,
            "error": {"code": "JOB_NOT_FOUND", "message": "Job not found"}
        }), 404
    
    status = download_status[job_id]
    if status['status'] != 'completed':
        return jsonify({
            "success": False,
            "error": {"code": "DOWNLOAD_NOT_READY", "message": f"Status: {status['status']}"}
        }), 400
    
    if job_id not in download_files:
        return jsonify({
            "success": False,
            "error": {"code": "FILE_NOT_FOUND", "message": "File expired"}
        }), 404
    
    file_path = download_files[job_id]
    if not os.path.exists(file_path):
        cleanup_download(job_id)
        return jsonify({
            "success": False,
            "error": {"code": "FILE_EXPIRED", "message": "File no longer available"}
        }), 404
    
    # Schedule cleanup
    def delayed_cleanup():
        time.sleep(120)
        cleanup_download(job_id)
    
    threading.Thread(target=delayed_cleanup, daemon=True).start()
    
    safe_filename = f"audio_{job_id[:8]}.mp3"
    response = send_file(
        file_path,
        as_attachment=True,
        download_name=safe_filename,
        mimetype='audio/mpeg'
    )
    
    response.headers['X-Job-ID'] = job_id
    return response

# Legacy endpoints
@app.route('/download/audio', methods=['POST'])
@rate_limit()
def download_audio():
    return download_audio_ultrafast()

# Error handlers
@app.errorhandler(429)
def handle_rate_limit(e):
    return jsonify({
        "success": False,
        "error": {"code": "RATE_LIMIT_EXCEEDED", "message": "Too many requests"}
    }), 429

@app.errorhandler(500)
def handle_internal_error(e):
    return jsonify({
        "success": False,
        "error": {"code": "INTERNAL_SERVER_ERROR", "message": "Server error"}
    }), 500

def periodic_cleanup():
    """Periodic cleanup worker"""
    while True:
        try:
            time.sleep(600)  # 10 minutes
            cleanup_old_downloads()
            gc.collect()
            
            # Refresh proxy list if needed
            if len(proxy_list) < 10:
                get_working_proxy()
                
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    
    # Start cleanup thread
    threading.Thread(target=periodic_cleanup, daemon=True).start()
    
    # Initialize proxy
    try:
        get_working_proxy()
        logger.info(f"Initialized with {len(proxy_list)} proxies")
    except Exception as e:
        logger.warning(f"Proxy init failed: {e}")
    
    logger.info("🎵 YouTube Audio Downloader - Optimized Version")
    logger.info(f"🚀 Starting server on host 0.0.0.0 port {port}")
    
    # Ensure we're binding to all interfaces
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
