import os
import tempfile
import time
import threading
import uuid
import gc
import psutil
import requests
import random
import re
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import yt_dlp
from pathlib import Path
import logging
from functools import wraps
from urllib.parse import urlparse

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for iOS

# Render.com free tier optimizations
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300  # Cache files for 5 minutes

# Global variables for tracking
download_status = {}
download_files = {}
active_downloads = 0
MAX_CONCURRENT_DOWNLOADS = 2

# Rate limiting storage (in-memory for free tier)
rate_limit_storage = {}

# Simple proxy management with rotation
current_proxy = None
proxy_last_fetched = 0
PROXY_UPDATE_INTERVAL = 1800  # 30 minutes instead of 1 hour
proxy_list = []
proxy_index = 0

def validate_youtube_url(url):
    """Validate and clean YouTube URL"""
    if not url or not isinstance(url, str):
        return None, "Invalid URL format"
    
    # Clean URL
    if "&list=" in url:
        url = url.split("&list=")[0]
    if "&t=" in url:
        url = url.split("&t=")[0]
    
    # Basic YouTube URL validation
    youtube_patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
        r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
    ]
    
    for pattern in youtube_patterns:
        if re.search(pattern, url):
            return url, None
    
    return None, "Not a valid YouTube URL"

def get_working_proxy():
    """Enhanced proxy fetching with better error handling and rotation"""
    global current_proxy, proxy_last_fetched, proxy_list, proxy_index
    
    current_time = time.time()
    
    # Check if we need to update proxy list
    if (current_time - proxy_last_fetched > PROXY_UPDATE_INTERVAL) or not proxy_list:
        try:
            logger.info("Fetching fresh proxy list...")
            proxy_urls = [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
                'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt'
            ]
            
            all_proxies = []
            for url in proxy_urls:
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        lines = [line.strip() for line in response.text.strip().split('\n')]
                        for line in lines:
                            if ':' in line and not line.startswith('#'):
                                # Extract IP:PORT
                                if line.count(':') == 1:
                                    all_proxies.append(line.strip())
                                elif line.count(':') > 1:
                                    # Handle formats like IP:PORT:country:etc
                                    parts = line.split(':')
                                    if len(parts) >= 2:
                                        all_proxies.append(f"{parts[0]}:{parts[1]}")
                    logger.info(f"Fetched {len(all_proxies)} proxies from {url}")
                except Exception as e:
                    logger.warning(f"Failed to fetch from {url}: {e}")
                    continue
            
            if all_proxies:
                # Validate and filter proxies
                valid_proxies = []
                for proxy in all_proxies:
                    try:
                        ip, port = proxy.split(':')
                        # Basic IP validation
                        if (len(ip.split('.')) == 4 and 
                            all(0 <= int(octet) <= 255 for octet in ip.split('.')) and
                            1 <= int(port) <= 65535):
                            valid_proxies.append(proxy)
                    except (ValueError, IndexError):
                        continue
                
                proxy_list = list(set(valid_proxies))  # Remove duplicates
                proxy_index = 0
                proxy_last_fetched = current_time
                logger.info(f"Updated proxy list with {len(proxy_list)} valid proxies")
            else:
                logger.warning("No proxies fetched, using existing list or direct connection")
                
        except Exception as e:
            logger.error(f"Error updating proxy list: {e}")
    
    # Try to find a working proxy from the list
    if proxy_list:
        # Test up to 5 proxies from the list
        attempts = min(5, len(proxy_list))
        for _ in range(attempts):
            if proxy_index >= len(proxy_list):
                proxy_index = 0
            
            proxy = proxy_list[proxy_index]
            test_proxy = f"http://{proxy}"
            
            if test_proxy_quick(test_proxy):
                current_proxy = test_proxy
                logger.info(f"Using working proxy: {current_proxy}")
                return current_proxy
            
            proxy_index += 1
        
        logger.warning("No working proxies found in current batch")
    
    current_proxy = None
    return current_proxy

def test_proxy_quick(proxy_url, timeout=8):
    """Quick proxy test with multiple endpoints"""
    try:
        proxies = {'http': proxy_url, 'https': proxy_url}
        
        # Test with a simple endpoint
        test_endpoints = [
            'http://httpbin.org/ip',
            'http://ip-api.com/json',
            'http://ifconfig.me/ip'
        ]
        
        for endpoint in test_endpoints:
            try:
                response = requests.get(endpoint, proxies=proxies, timeout=timeout)
                if response.status_code == 200 and len(response.text.strip()) > 5:
                    return True
            except:
                continue
        
        return False
    except Exception:
        return False

def rate_limit(max_requests=15, window=300):  # More lenient rate limiting
    """Enhanced rate limiting with better error handling"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            user_agent = request.headers.get('User-Agent', '')
            now = time.time()
            
            # More lenient for iOS apps
            if any(ua in user_agent.lower() for ua in ['ios', 'iphone', 'ipad', 'cfnetwork']):
                max_requests_adjusted = max_requests + 5
            else:
                max_requests_adjusted = max_requests
            
            # Clean old entries
            if client_ip in rate_limit_storage:
                rate_limit_storage[client_ip] = [
                    timestamp for timestamp in rate_limit_storage[client_ip]
                    if now - timestamp < window
                ]
            else:
                rate_limit_storage[client_ip] = []
            
            # Check rate limit
            current_requests = len(rate_limit_storage[client_ip])
            if current_requests >= max_requests_adjusted:
                logger.warning(f"Rate limit exceeded for {client_ip}: {current_requests}/{max_requests_adjusted}")
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": "Too many requests. Please wait before trying again.",
                        "retry_after": window,
                        "current_requests": current_requests,
                        "limit": max_requests_adjusted
                    }
                }), 429
            
            # Add current request
            rate_limit_storage[client_ip].append(now)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def check_system_resources():
    """Enhanced resource checking with better error messages"""
    global active_downloads
    
    try:
        if active_downloads >= MAX_CONCURRENT_DOWNLOADS:
            return False, "TOO_MANY_DOWNLOADS", "Server is processing maximum concurrent downloads"
        
        # Check memory
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 85:
            cleanup_old_downloads(force=True)
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 90:
                return False, "HIGH_MEMORY_USAGE", "Server is under high memory load"
        
        # Check disk space
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        if free_gb < 0.5:
            cleanup_old_downloads(force=True)
            disk_usage = psutil.disk_usage('/tmp')
            free_gb = disk_usage.free / (1024**3)
            if free_gb < 0.2:
                return False, "LOW_DISK_SPACE", "Insufficient disk space available"
        
        return True, "OK", "Resources available"
    except Exception as e:
        logger.error(f"Resource check failed: {e}")
        return True, "OK", "Resource check bypassed due to error"

def check_memory_usage():
    """Check memory usage and trigger cleanup if needed"""
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 80:
            logger.warning(f"High memory usage: {memory_percent}%")
            cleanup_old_downloads(force=True)
            gc.collect()
            return True
    except Exception as e:
        logger.error(f"Memory check failed: {e}")
    return False

def cleanup_old_downloads(force=False, max_age=1800):
    """Aggressive cleanup for free tier"""
    try:
        current_time = time.time()
        to_delete = []
        
        for job_id, status_info in download_status.items():
            age_limit = 300 if force else max_age
            if current_time - status_info.get('created_at', current_time) > age_limit:
                to_delete.append(job_id)
        
        for job_id in to_delete:
            cleanup_download(job_id, silent=True)
        
        if to_delete:
            logger.info(f"Cleaned up {len(to_delete)} old downloads")
            
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

@app.route('/', methods=['GET'])
def health_check():
    """Enhanced health check"""
    try:
        memory_percent = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        
        status = {
            "success": True,
            "data": {
                "service": "youtube-audio-downloader",
                "status": "healthy",
                "version": "2.1-fixed",
                "active_downloads": active_downloads,
                "memory_usage": f"{memory_percent:.1f}%",
                "free_disk_gb": f"{free_gb:.2f}",
                "total_jobs": len(download_status),
                "proxy_status": "active" if current_proxy else "direct",
                "proxy_count": len(proxy_list),
                "endpoints": {
                    "ultrafast": "/download/audio/ultrafast",
                    "async": "/download/audio/async",
                    "status": "/download/status/{job_id}",
                    "file": "/download/file/{job_id}"
                }
            }
        }
        
        if memory_percent > 90 or free_gb < 0.1:
            status["data"]["status"] = "degraded"
            
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "success": True,
            "data": {
                "service": "youtube-audio-downloader",
                "status": "healthy"
            }
        }), 200

@app.route('/download/audio/ultrafast', methods=['POST'])
@rate_limit(max_requests=8, window=300)  # More lenient rate limiting
def download_audio_ultrafast():
    """Ultra-fast download with enhanced error handling"""
    global active_downloads
    
    # Enhanced resource checking
    can_proceed, error_code, error_message = check_system_resources()
    if not can_proceed:
        return jsonify({
            "success": False,
            "error": {
                "code": error_code,
                "message": error_message,
                "suggestion": "Try again in a few moments"
            }
        }), 503
    
    if not request.is_json:
        return jsonify({
            "success": False,
            "error": {
                "code": "INVALID_REQUEST",
                "message": "Request must be JSON format"
            }
        }), 400
    
    try:
        data = request.json
        youtube_url = data.get('url')
        
        if not youtube_url:
            return jsonify({
                "success": False,
                "error": {
                    "code": "MISSING_URL",
                    "message": "URL parameter is required"
                }
            }), 400
        
        # Validate URL
        clean_url, url_error = validate_youtube_url(youtube_url)
        if url_error:
            return jsonify({
                "success": False,
                "error": {
                    "code": "INVALID_URL",
                    "message": url_error
                }
            }), 400
        
        logger.info(f"Processing download for: {clean_url}")
        
        active_downloads += 1
        temp_dir = None
        
        try:
            # Aggressive cleanup before starting
            cleanup_old_downloads(force=True)
            
            # Create temp directory
            temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_')
            
            # Get proxy
            proxy_url = get_working_proxy()
            
            # Ultra-optimized options for free tier
            ydl_opts = {
                'format': 'worstaudio[abr>=64]/bestaudio[abr<=128]/bestaudio[ext=m4a][abr<=128]',
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
                    '-threads', '2',
                    '-preset', 'ultrafast',
                ],
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
                'retries': 1,
                'extractor_retries': 1,
                'writesubtitles': False,
                'writeautomaticsub': False,
                'embed_subs': False,
                'writeinfojson': False,
                'writethumbnail': False,
                'extract_flat': False,
                'no_check_certificate': True,
            }
            
            # Add proxy if available
            if proxy_url:
                ydl_opts['proxy'] = proxy_url
                logger.info(f"Using proxy: {proxy_url}")
            
            def attempt_download(opts, retry_count=0):
                """Attempt download with error handling"""
                try:
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        ydl.download([clean_url])
                        return True, None
                except Exception as e:
                    error_str = str(e).lower()
                    logger.warning(f"Download attempt {retry_count + 1} failed: {e}")
                    
                    # Handle specific errors
                    if "429" in error_str or "too many requests" in error_str:
                        return False, ("YOUTUBE_RATE_LIMIT", "YouTube is rate limiting requests. Try again later.")
                    elif "proxy" in error_str or "tunnel connection failed" in error_str:
                        return False, ("PROXY_ERROR", str(e))
                    elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
                        return False, ("VIDEO_UNAVAILABLE", "Video is unavailable, private, or has been removed")
                    elif "network" in error_str or "connection" in error_str or "timeout" in error_str:
                        return False, ("NETWORK_ERROR", "Network connection problem")
                    else:
                        return False, ("DOWNLOAD_ERROR", str(e))
            
            # First attempt with proxy (if available)
            success, error_info = attempt_download(ydl_opts, 0)
            
            # If proxy failed, retry without proxy
            if not success and error_info and error_info[0] == "PROXY_ERROR" and 'proxy' in ydl_opts:
                logger.info("Retrying without proxy...")
                del ydl_opts['proxy']
                success, error_info = attempt_download(ydl_opts, 1)
            
            if not success:
                error_code, error_message = error_info if error_info else ("UNKNOWN_ERROR", "Download failed")
                return jsonify({
                    "success": False,
                    "error": {
                        "code": error_code,
                        "message": error_message
                    }
                }), 500 if error_code != "YOUTUBE_RATE_LIMIT" else 429
            
            # Find and return file
            for pattern in ['*.mp3', '*.m4a', '*.webm', '*.opus']:
                found_files = list(Path(temp_dir).glob(pattern))
                if found_files:
                    file_path = str(found_files[0])
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    safe_filename = f"audio_ultrafast_{int(time.time())}.mp3"
                    
                    def cleanup_after_send():
                        time.sleep(60)
                        try:
                            import shutil
                            shutil.rmtree(temp_dir, ignore_errors=True)
                        except Exception as e:
                            logger.error(f"Cleanup error: {e}")
                    
                    cleanup_thread = threading.Thread(target=cleanup_after_send)
                    cleanup_thread.daemon = True
                    cleanup_thread.start()
                    
                    # Create response
                    response = send_file(
                        file_path,
                        as_attachment=True,
                        download_name=safe_filename,
                        mimetype='audio/mpeg'
                    )
                    
                    # Add headers for iOS
                    response.headers['X-File-Size'] = str(file_size)
                    response.headers['X-File-Size-MB'] = str(round(file_size / (1024 * 1024), 2))
                    
                    return response
            
            return jsonify({
                "success": False,
                "error": {
                    "code": "NO_AUDIO_FILE",
                    "message": "Download completed but no audio file was created"
                }
            }), 500
        
        finally:
            active_downloads -= 1
            if temp_dir and os.path.exists(temp_dir):
                try:
                    import shutil
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception as e:
                    logger.error(f"Final cleanup error: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error in ultrafast download: {e}")
        return jsonify({
            "success": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }), 500

@app.route('/download/audio/async', methods=['POST'])
@rate_limit(max_requests=5, window=600)
def download_audio_async():
    """Async download with enhanced error handling"""
    can_proceed, error_code, error_message = check_system_resources()
    if not can_proceed:
        return jsonify({
            "success": False,
            "error": {
                "code": error_code,
                "message": error_message
            }
        }), 503
    
    if not request.is_json:
        return jsonify({
            "success": False,
            "error": {
                "code": "INVALID_REQUEST",
                "message": "Request must be JSON format"
            }
        }), 400
    
    try:
        data = request.json
        youtube_url = data.get('url')
        
        if not youtube_url:
            return jsonify({
                "success": False,
                "error": {
                    "code": "MISSING_URL",
                    "message": "URL parameter is required"
                }
            }), 400
        
        # Validate URL
        clean_url, url_error = validate_youtube_url(youtube_url)
        if url_error:
            return jsonify({
                "success": False,
                "error": {
                    "code": "INVALID_URL",
                    "message": url_error
                }
            }), 400
        
        # Cleanup before starting
        cleanup_old_downloads()
        
        job_id = str(uuid.uuid4())
        
        download_status[job_id] = {
            'status': 'queued',
            'progress': 0,
            'message': 'Download queued',
            'created_at': time.time(),
            'url': clean_url
        }
        
        thread = threading.Thread(target=background_download_enhanced, args=(job_id, clean_url))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            "success": True,
            "data": {
                "job_id": job_id,
                "status": "queued",
                "check_url": f"/download/status/{job_id}",
                "download_url": f"/download/file/{job_id}",
                "estimated_time": "30-120 seconds"
            }
        }), 202
    
    except Exception as e:
        logger.error(f"Error in async download: {e}")
        return jsonify({
            "success": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to start download"
            }
        }), 500

def background_download_enhanced(job_id, youtube_url):
    """Enhanced background download with better error handling"""
    global active_downloads
    active_downloads += 1
    temp_dir = None
    
    try:
        download_status[job_id].update({
            'status': 'processing',
            'progress': 10,
            'message': 'Starting download...',
            'started_at': time.time()
        })
        
        # Check memory before proceeding
        if check_memory_usage():
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'Server overloaded. Try again later.',
                'error_code': 'SERVER_OVERLOADED'
            })
            return
        
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_async_')
        
        download_status[job_id].update({
            'progress': 25,
            'message': 'Downloading audio...'
        })
        
        # Get proxy
        proxy_url = get_working_proxy()
        
        # Enhanced options for async download
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
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            'concurrent_fragment_downloads': 3,
            'http_chunk_size': 1048576,
            'buffer_size': 16384,
            'no_color': True,
            'quiet': False,
            'socket_timeout': 30,
            'fragment_retries': 2,
            'retries': 2,
            'extractor_retries': 1,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
        }
        
        if proxy_url:
            ydl_opts['proxy'] = proxy_url
        
        download_status[job_id].update({
            'progress': 50,
            'message': 'Processing audio...'
        })
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])
                
                # Find downloaded file
                for pattern in ['*.mp3', '*.m4a', '*.webm', '*.opus']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        file_size = os.path.getsize(file_path)
                        download_files[job_id] = file_path
                        
                        download_status[job_id].update({
                            'status': 'completed',
                            'progress': 100,
                            'message': 'Download completed successfully',
                            'completed_at': time.time(),
                            'file_size': file_size,
                            'file_size_mb': round(file_size / (1024 * 1024), 2)
                        })
                        
                        # Auto-cleanup after 30 minutes
                        cleanup_timer = threading.Timer(1800, cleanup_download, args=(job_id,))
                        cleanup_timer.daemon = True
                        cleanup_timer.start()
                        return
                
                download_status[job_id].update({
                    'status': 'failed',
                    'progress': 0,
                    'message': 'No audio file was created',
                    'error_code': 'NO_AUDIO_FILE'
                })
        
        except Exception as e:
            error_msg = str(e).lower()
            if "429" in error_msg or "too many requests" in error_msg:
                error_code = "YOUTUBE_RATE_LIMIT"
                message = "YouTube is rate limiting requests. Try again later."
            elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted", "removed"]):
                error_code = "VIDEO_UNAVAILABLE"
                message = "Video is unavailable, private, or has been removed"
            elif "network" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                error_code = "NETWORK_ERROR"
                message = "Network connection problem"
            else:
                error_code = "DOWNLOAD_ERROR"
                message = f"Download failed: {str(e)}"
            
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': message,
                'error_code': error_code,
                'failed_at': time.time()
            })
    
    except Exception as e:
        logger.error(f"Unexpected error in background download {job_id}: {e}")
        download_status[job_id].update({
            'status': 'failed',
            'progress': 0,
            'message': 'An unexpected error occurred',
            'error_code': 'INTERNAL_ERROR',
            'failed_at': time.time()
        })
    
    finally:
        active_downloads -= 1
        if temp_dir and job_id not in download_files:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Background cleanup error: {e}")

@app.route('/download/status/<job_id>', methods=['GET'])
def check_download_status(job_id):
    """Enhanced status check"""
    if job_id not in download_status:
        return jsonify({
            "success": False,
            "error": {
                "code": "JOB_NOT_FOUND",
                "message": "Job not found or has expired"
            }
        }), 404
    
    status = download_status[job_id].copy()
    
    # Add timing information
    if 'created_at' in status:
        current_time = time.time()
        age = current_time - status['created_at']
        remaining = max(0, 1800 - age)  # 30 minutes total
        
        status['timing'] = {
            'age_seconds': int(age),
            'expires_in_seconds': int(remaining),
            'expires_in_minutes': round(remaining / 60, 1)
        }
    
    return jsonify({
        "success": True,
        "data": status
    })

@app.route('/download/file/<job_id>', methods=['GET'])
def get_download_file(job_id):
    """Enhanced file download"""
    if job_id not in download_status:
        return jsonify({
            "success": False,
            "error": {
                "code": "JOB_NOT_FOUND",
                "message": "Job not found or has expired"
            }
        }), 404
    
    status = download_status[job_id]
    if status['status'] != 'completed':
        return jsonify({
            "success": False,
            "error": {
                "code": "DOWNLOAD_NOT_READY",
                "message": f"Download status: {status['status']}",
                "current_status": status
            }
        }), 400
    
    if job_id not in download_files:
        return jsonify({
            "success": False,
            "error": {
                "code": "FILE_NOT_FOUND",
                "message": "File has expired or been cleaned up"
            }
        }), 404
    
    file_path = download_files[job_id]
    if not os.path.exists(file_path):
        cleanup_download(job_id, silent=True)
        return jsonify({
            "success": False,
            "error": {
                "code": "FILE_EXPIRED",
                "message": "File no longer available"
            }
        }), 404
    
    # Schedule cleanup after download
    def delayed_cleanup():
        time.sleep(120)  # Wait 2 minutes
        cleanup_download(job_id, silent=True)
    
    cleanup_thread = threading.Thread(target=delayed_cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    
    file_size = os.path.getsize(file_path)
    safe_filename = f"audio_hq_{job_id[:8]}.mp3"
    
    response = send_file(
        file_path,
        as_attachment=True,
        download_name=safe_filename,
        mimetype='audio/mpeg'
    )
    
    # Add headers for iOS
    response.headers['X-File-Size'] = str(file_size)
    response.headers['X-File-Size-MB'] = str(round(file_size / (1024 * 1024), 2))
    response.headers['X-Job-ID'] = job_id
    
    return response

def cleanup_download(job_id, silent=False):
    """Enhanced cleanup function"""
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

# Legacy endpoints for backward compatibility
@app.route('/download/audio', methods=['POST'])
@rate_limit()
def download_audio():
    """Legacy endpoint - redirects to ultrafast"""
    return download_audio_ultrafast()

@app.route('/download/audio/fast', methods=['POST'])
@rate_limit()
def download_audio_fast():
    """Legacy fast endpoint"""
    return download_audio_ultrafast()

# Enhanced error handlers
@app.errorhandler(429)
def handle_rate_limit_error(e):
    return jsonify({
        "success": False,
        "error": {
            "code": "RATE_LIMIT_EXCEEDED",
            "message": "Too many requests. Please wait before trying again.",
            "retry_after": 300,
            "suggestion": "Wait a few minutes before making another request"
        }
    }), 429

@app.errorhandler(500)
def handle_internal_error(e):
    logger.error(f"Internal server error: {e}")
    return jsonify({
        "success": False,
        "error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred on the server",
            "suggestion": "Try again in a few moments"
        }
    }), 500

@app.errorhandler(413)
def handle_file_too_large(e):
    return jsonify({
        "success": False,
        "error": {
            "code": "FILE_TOO_LARGE",
            "message": "File too large for free tier",
            "limit": "100MB maximum"
        }
    }), 413

@app.errorhandler(400)
def handle_bad_request(e):
    return jsonify({
        "success": False,
        "error": {
            "code": "BAD_REQUEST",
            "message": "Invalid request format or parameters"
        }
    }), 400

def periodic_cleanup():
    """Enhanced periodic cleanup"""
    while True:
        try:
            time.sleep(600)  # 10 minutes
            cleanup_old_downloads()
            gc.collect()
            
            # Log status
            active_jobs = len(download_status)
            proxy_status = "active" if current_proxy else "direct"
            logger.info(f"Periodic cleanup completed. Active jobs: {active_jobs}, Proxy: {proxy_status}")
            
            # Refresh proxy list periodically
            if len(proxy_list) < 10:  # If proxy list is getting small
                logger.info("Proxy list running low, triggering refresh...")
                get_working_proxy()
                
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    
    # Free tier optimizations
    os.environ['FFMPEG_THREADS'] = '2'
    os.environ['MALLOC_ARENA_MAX'] = '1'
    
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    # Initialize proxy on startup
    logger.info("Initializing proxy system...")
    try:
        get_working_proxy()
        logger.info(f"Proxy initialization complete. Found {len(proxy_list)} proxies.")
    except Exception as e:
        logger.warning(f"Proxy initialization failed: {e}")
    
    # System checks
    try:
        import subprocess
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True, text=True)
        logger.info("✅ FFmpeg is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("❌ FFmpeg not found - downloads may fail")
    
    try:
        logger.info(f"✅ yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.warning("⚠️ Could not determine yt-dlp version")
    
    logger.info("=" * 60)
    logger.info("🎵 YOUTUBE AUDIO DOWNLOADER SERVER v2.1-FIXED")
    logger.info("🔧 Fixed Error 429/500 Issues for iOS")
    logger.info("🏠 Optimized for Render.com FREE TIER")
    logger.info("=" * 60)
    
    logger.info("📡 Available endpoints:")
    logger.info("  • POST /download/audio/ultrafast    - Ultra-fast download (128kbps)")
    logger.info("  • POST /download/audio/async        - Background download (192kbps)")
    logger.info("  • GET  /download/status/{job_id}    - Check download status")
    logger.info("  • GET  /download/file/{job_id}      - Download completed file")
    logger.info("  • GET  /                            - Health check")
    
    logger.info("🔧 Key Fixes Applied:")
    logger.info("  • ✅ Enhanced proxy rotation system")
    logger.info("  • ✅ Better YouTube rate limit handling")
    logger.info("  • ✅ Improved error detection and recovery")
    logger.info("  • ✅ More lenient rate limiting for iOS")
    logger.info("  • ✅ Enhanced URL validation")
    logger.info("  • ✅ Better resource management")
    logger.info("  • ✅ CORS enabled for iOS apps")
    logger.info("  • ✅ Structured JSON error responses")
    logger.info("  • ✅ Multiple retry mechanisms")
    logger.info("  • ✅ Enhanced logging and monitoring")
    
    if proxy_list:
        logger.info(f"🌐 Proxy system: {len(proxy_list)} proxies loaded")
        if current_proxy:
            logger.info(f"🌐 Current proxy: {current_proxy}")
    else:
        logger.info("🌐 Direct connection mode")
    
    if os.environ.get('RENDER'):
        logger.info("🚀 Running on Render.com with enhanced error handling")
    
    logger.info("=" * 60)
    logger.info("🟢 Server starting with error fixes...")
    
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
