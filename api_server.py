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
from urllib.parse import urlparse, parse_qs
import hashlib

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for iOS app

# Render.com free tier optimizations
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300  # Cache files for 5 minutes

# Global variables for tracking
download_status = {}
download_files = {}
download_metadata = {}
active_downloads = 0
MAX_CONCURRENT_DOWNLOADS = 1  # Reduced from 2 to 1 for free tier

# Rate limiting storage (in-memory for free tier)
rate_limit_storage = {}

# Simple proxy management
current_proxy = None
proxy_last_fetched = 0
PROXY_UPDATE_INTERVAL = 3600  # 1 hour
USE_PROXY = os.environ.get('USE_PROXY', 'false').lower() == 'true'

def extract_video_id(url):
    """Extract video ID from various YouTube URL formats"""
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
        r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None

def validate_youtube_url(url):
    """Validate and clean YouTube URL"""
    if not url or not isinstance(url, str):
        return None, "Invalid URL format"
    
    # Clean URL
    if "&list=" in url:
        url = url.split("&list=")[0]
    
    # Remove timestamp parameters that might cause issues
    if "&t=" in url:
        url = url.split("&t=")[0]
    
    video_id = extract_video_id(url)
    if not video_id:
        return None, "Not a valid YouTube URL"
    
    # Return clean URL
    clean_url = f"https://www.youtube.com/watch?v={video_id}"
    return clean_url, None

def get_working_proxy():
    """Enhanced proxy fetching - only when USE_PROXY is enabled"""
    global current_proxy, proxy_last_fetched
    
    # Skip proxy functionality if disabled or on Render startup
    if not USE_PROXY:
        return None
        
    # Skip proxy fetch on Render during startup to prevent deployment timeout
    if os.environ.get('RENDER') and current_proxy is None and proxy_last_fetched == 0:
        logger.info("Skipping proxy initialization on Render deployment")
        return None
    
    current_time = time.time()
    if (current_time - proxy_last_fetched > PROXY_UPDATE_INTERVAL) or (current_proxy is None):
        try:
            # Primary source: TheSpeedX/PROXY-List (GitHub)
            proxy_urls = [
                'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
                'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt'  # Backup source
            ]
            
            all_proxies = []
            
            for url in proxy_urls:
                try:
                    logger.info(f"Fetching proxies from: {url}")
                    response = requests.get(url, timeout=10)  # Reduced timeout
                    if response.status_code == 200:
                        # TheSpeedX format: IP:PORT per line
                        proxies = [line.strip() for line in response.text.strip().split('\n') 
                                 if ':' in line.strip() and line.strip().count(':') == 1
                                 and not line.strip().startswith('#')]
                        
                        all_proxies.extend(proxies)
                        logger.info(f"Fetched {len(proxies)} proxies from {url}")
                        break  # Use first successful source only for faster startup
                except Exception as e:
                    logger.warning(f"Failed to fetch from {url}: {e}")
                    continue
            
            if all_proxies:
                # Remove duplicates and validate
                unique_proxies = list(set(all_proxies))
                valid_proxies = []
                
                for proxy in unique_proxies[:50]:  # Limit to first 50 for faster processing
                    try:
                        ip, port = proxy.split(':')
                        if (len(ip.split('.')) == 4 and 
                            all(0 <= int(octet) <= 255 for octet in ip.split('.')) and
                            1 <= int(port) <= 65535):
                            valid_proxies.append(proxy)
                    except (ValueError, IndexError):
                        continue
                
                if valid_proxies:
                    # Test fewer proxies for faster response
                    test_count = min(3, len(valid_proxies))  # Reduced from 8 to 3
                    test_proxies = random.sample(valid_proxies, test_count)
                    
                    for proxy in test_proxies:
                        test_proxy = f"http://{proxy}"
                        if test_proxy_quick(test_proxy, timeout=5):  # Reduced timeout
                            current_proxy = test_proxy
                            proxy_last_fetched = current_time
                            logger.info(f"Found working proxy: {current_proxy}")
                            return current_proxy
                    
                    logger.warning("No working proxies found, using direct connection")
                
                current_proxy = None
            else:
                current_proxy = None
                
        except Exception as e:
            logger.error(f"Error in proxy fetching: {e}")
            current_proxy = None
    
    return current_proxy

def test_proxy_quick(proxy_url, timeout=5):
    """Quick proxy testing with single endpoint"""
    try:
        proxies = {'http': proxy_url, 'https': proxy_url}
        response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=timeout)
        return response.status_code == 200 and 'origin' in response.text
    except Exception:
        return False

def rate_limit(max_requests=8, window=300):
    """Simple in-memory rate limiting with iOS considerations"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            user_agent = request.headers.get('User-Agent', '')
            
            # More lenient rate limiting for iOS apps
            if 'iOS' in user_agent or 'iPhone' in user_agent or 'iPad' in user_agent:
                max_requests = 12  # Higher limit for iOS
            
            now = time.time()
            
            if client_ip in rate_limit_storage:
                rate_limit_storage[client_ip] = [
                    timestamp for timestamp in rate_limit_storage[client_ip]
                    if now - timestamp < window
                ]
            else:
                rate_limit_storage[client_ip] = []
            
            if len(rate_limit_storage[client_ip]) >= max_requests:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": "Rate limit exceeded. Try again later.",
                        "retry_after": window
                    }
                }), 429
            
            rate_limit_storage[client_ip].append(now)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def check_system_resources():
    """Check if system has enough resources for download"""
    global active_downloads
    
    if active_downloads >= MAX_CONCURRENT_DOWNLOADS:
        return False, "TOO_MANY_DOWNLOADS", "Server is processing maximum concurrent downloads"
    
    try:
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > 85:
            cleanup_old_downloads(force=True)
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 90:
                return False, "HIGH_MEMORY_USAGE", "Server is under high memory load"
    except Exception:
        pass
    
    try:
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        if free_gb < 0.3:
            cleanup_old_downloads(force=True)
            disk_usage = psutil.disk_usage('/tmp')
            free_gb = disk_usage.free / (1024**3)
            if free_gb < 0.1:
                return False, "LOW_DISK_SPACE", "Insufficient disk space available"
    except Exception:
        pass
    
    return True, "OK", "Resources available"

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

def aggressive_cleanup():
    """More aggressive cleanup for free tier startup"""
    try:
        import shutil
        temp_dirs = ['/tmp', '/var/tmp']
        
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                for item in os.listdir(temp_dir):
                    if item.startswith('yt_') or item.startswith('tmp'):
                        item_path = os.path.join(temp_dir, item)
                        try:
                            if os.path.isdir(item_path):
                                shutil.rmtree(item_path, ignore_errors=True)
                            else:
                                os.remove(item_path)
                        except:
                            pass
        logger.info("Aggressive cleanup completed")
    except Exception as e:
        logger.warning(f"Aggressive cleanup failed: {e}")

@app.route('/', methods=['GET'])
def health_check():
    """iOS-friendly health check with detailed status"""
    try:
        memory_percent = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        
        status = {
            "success": True,
            "data": {
                "service": "youtube-audio-downloader",
                "version": "2.0-ios-render",
                "status": "healthy",
                "server_info": {
                    "active_downloads": active_downloads,
                    "total_jobs": len(download_status),
                    "memory_usage_percent": round(memory_percent, 1),
                    "free_disk_gb": round(free_gb, 2),
                    "proxy_status": "active" if current_proxy else "direct",
                    "max_concurrent": MAX_CONCURRENT_DOWNLOADS,
                    "proxy_enabled": USE_PROXY
                },
                "endpoints": {
                    "quick_download": "/api/v1/download/quick",
                    "background_download": "/api/v1/download/background",
                    "video_info": "/api/v1/video/info",
                    "download_status": "/api/v1/download/status/{job_id}",
                    "download_file": "/api/v1/download/file/{job_id}"
                }
            }
        }
        
        if memory_percent > 90 or free_gb < 0.1:
            status["data"]["status"] = "degraded"
            status["data"]["warning"] = "Server under high load"
            
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "success": True,
            "data": {
                "service": "youtube-audio-downloader",
                "status": "healthy",
                "version": "2.0-ios-render"
            }
        }), 200

@app.route('/api/v1/video/info', methods=['POST'])
@rate_limit(max_requests=15, window=300)
def get_video_info():
    """Get video information without downloading - iOS optimized"""
    try:
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": {
                    "code": "INVALID_REQUEST",
                    "message": "Request must be JSON format"
                }
            }), 400
        
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
        
        clean_url, error = validate_youtube_url(youtube_url)
        if error:
            return jsonify({
                "success": False,
                "error": {
                    "code": "INVALID_URL",
                    "message": error
                }
            }), 400
        
        # Quick info extraction
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'skip_download': True,
            'socket_timeout': 15,
        }
        
        # Only add proxy if enabled and available
        if USE_PROXY:
            proxy_url = get_working_proxy()
            if proxy_url:
                ydl_opts['proxy'] = proxy_url
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(clean_url, download=False)
            
            # Extract iOS-friendly info
            video_info = {
                "id": info.get('id'),
                "title": info.get('title', 'Unknown Title'),
                "duration": info.get('duration', 0),
                "duration_string": info.get('duration_string', '0:00'),
                "uploader": info.get('uploader', 'Unknown'),
                "view_count": info.get('view_count', 0),
                "upload_date": info.get('upload_date'),
                "thumbnail": info.get('thumbnail'),
                "description": info.get('description', '')[:500] + '...' if info.get('description', '') else '',
                "availability": info.get('availability', 'unknown')
            }
            
            # Estimate file sizes for different qualities
            formats_info = []
            if info.get('formats'):
                for fmt in info.get('formats', []):
                    if fmt.get('acodec') != 'none' and fmt.get('vcodec') == 'none':  # Audio only
                        formats_info.append({
                            "format_id": fmt.get('format_id'),
                            "ext": fmt.get('ext'),
                            "abr": fmt.get('abr'),
                            "filesize": fmt.get('filesize'),
                            "filesize_approx": fmt.get('filesize_approx')
                        })
            
            return jsonify({
                "success": True,
                "data": {
                    "video": video_info,
                    "available_formats": formats_info[:5],  # Limit to 5 formats
                    "estimated_sizes": {
                        "mp3_128k": f"{(info.get('duration', 0) * 16)}KB" if info.get('duration') else "Unknown",
                        "mp3_192k": f"{(info.get('duration', 0) * 24)}KB" if info.get('duration') else "Unknown"
                    }
                }
            }), 200
            
    except Exception as e:
        error_str = str(e).lower()
        if "unavailable" in error_str or "private" in error_str:
            return jsonify({
                "success": False,
                "error": {
                    "code": "VIDEO_UNAVAILABLE",
                    "message": "Video is unavailable, private, or doesn't exist"
                }
            }), 400
        else:
            return jsonify({
                "success": False,
                "error": {
                    "code": "INFO_EXTRACTION_FAILED",
                    "message": f"Could not extract video information: {str(e)}"
                }
            }), 500

@app.route('/api/v1/download/quick', methods=['POST'])
@rate_limit(max_requests=5, window=300)
def download_audio_quick():
    """Ultra-fast download optimized for iOS and Render free tier"""
    global active_downloads
    
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
    
    data = request.json
    youtube_url = data.get('url')
    quality = data.get('quality', 'standard')  # standard, high, ultra_fast

    if not youtube_url:
        return jsonify({
            "success": False,
            "error": {
                "code": "MISSING_URL",
                "message": "URL parameter is required"
            }
        }), 400

    clean_url, error = validate_youtube_url(youtube_url)
    if error:
        return jsonify({
            "success": False,
            "error": {
                "code": "INVALID_URL",
                "message": error
            }
        }), 400
    
    active_downloads += 1
    temp_dir = None
    
    try:
        cleanup_old_downloads(force=True)
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_quick_')
        
        # Quality-based settings optimized for free tier
        quality_settings = {
            'ultra_fast': {
                'format': 'worstaudio[abr>=64]/bestaudio[abr<=96]',
                'quality': '96',
                'sample_rate': '22050',
                'channels': '1'
            },
            'standard': {
                'format': 'bestaudio[abr<=128]/bestaudio[ext=m4a][abr<=128]',
                'quality': '128',
                'sample_rate': '44100',
                'channels': '2'
            },
            'high': {
                'format': 'bestaudio[abr<=192]/bestaudio[ext=m4a][abr<=192]',
                'quality': '192',
                'sample_rate': '44100',
                'channels': '2'
            }
        }
        
        settings = quality_settings.get(quality, quality_settings['standard'])
        
        ydl_opts = {
            'format': settings['format'],
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': settings['quality'],
            }],
            'postprocessor_args': [
                '-ar', settings['sample_rate'],
                '-ac', settings['channels'],
                '-b:a', f"{settings['quality']}k",
                '-threads', '1',  # Reduced for free tier
                '-preset', 'ultrafast' if quality == 'ultra_fast' else 'fast',
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            'concurrent_fragment_downloads': 1,  # Reduced for free tier
            'http_chunk_size': 256000,  # Reduced for free tier
            'buffer_size': 4096,  # Reduced for free tier
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

        # Add proxy only if enabled
        if USE_PROXY:
            proxy_url = get_working_proxy()
            if proxy_url:
                ydl_opts['proxy'] = proxy_url

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Get video info first for metadata
                info = ydl.extract_info(clean_url, download=False)
                
                # Download
                ydl.download([clean_url])
                
                # Find downloaded file
                for pattern in ['*.mp3', '*.m4a', '*.webm']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        
                        # Create safe filename
                        safe_title = re.sub(r'[^\w\s-]', '', info.get('title', 'audio'))[:50]
                        safe_filename = f"{safe_title}_{quality}.mp3"
                        
                        # Get file size
                        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                        
                        def cleanup_after_send():
                            time.sleep(60)
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except Exception:
                                pass
                        
                        cleanup_thread = threading.Thread(target=cleanup_after_send)
                        cleanup_thread.daemon = True
                        cleanup_thread.start()
                        
                        # Add metadata headers for iOS
                        response = send_file(
                            file_path,
                            as_attachment=True,
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                        
                        # Add custom headers for iOS
                        response.headers['X-File-Size'] = str(file_size)
                        response.headers['X-Video-Title'] = info.get('title', 'Unknown')
                        response.headers['X-Video-Duration'] = str(info.get('duration', 0))
                        response.headers['X-Quality'] = quality
                        
                        return response
                
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "NO_AUDIO_FILE",
                        "message": "Download completed but no audio file was created"
                    }
                }), 500
        
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle specific errors
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "RATE_LIMITED_BY_YOUTUBE",
                        "message": "Rate limited by YouTube. Try again in a few minutes."
                    }
                }), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted"]):
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "VIDEO_UNAVAILABLE",
                        "message": "Video is unavailable, private, or has been deleted"
                    }
                }), 400
            else:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "DOWNLOAD_FAILED",
                        "message": f"Download failed: {str(e)}"
                    }
                }), 500
    
    finally:
        active_downloads -= 1
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

@app.route('/api/v1/download/background', methods=['POST'])
@rate_limit(max_requests=3, window=600)
def download_audio_background():
    """Background download with progress tracking for iOS"""
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
    
    data = request.json
    youtube_url = data.get('url')
    quality = data.get('quality', 'high')
    
    if not youtube_url:
        return jsonify({
            "success": False,
            "error": {
                "code": "MISSING_URL",
                "message": "URL parameter is required"
            }
        }), 400

    clean_url, error = validate_youtube_url(youtube_url)
    if error:
        return jsonify({
            "success": False,
            "error": {
                "code": "INVALID_URL",
                "message": error
            }
        }), 400
    
    cleanup_old_downloads()
    job_id = str(uuid.uuid4())
    
    download_status[job_id] = {
        'status': 'queued',
        'progress': 0,
        'message': 'Download queued',
        'created_at': time.time(),
        'quality': quality,
        'url': clean_url
    }
    
    thread = threading.Thread(target=background_download_ios, args=(job_id, clean_url, quality))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "success": True,
        "data": {
            "job_id": job_id,
            "status": "queued",
            "estimated_time": "30-120 seconds",
            "endpoints": {
                "status_check": f"/api/v1/download/status/{job_id}",
                "download_file": f"/api/v1/download/file/{job_id}"
            },
            "note": "Files auto-delete after 30 minutes"
        }
    }), 202

def background_download_ios(job_id, youtube_url, quality):
    """iOS-optimized background download with detailed progress"""
    global active_downloads
    active_downloads += 1
    temp_dir = None
    
    try:
        download_status[job_id].update({
            'status': 'processing',
            'progress': 10,
            'message': 'Initializing download...',
            'started_at': time.time()
        })
        
        if check_memory_usage():
            download_status[job_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'Server overloaded. Try again later.',
                'error_code': 'SERVER_OVERLOADED'
            })
            return
        
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_bg_')
        
        download_status[job_id].update({
            'progress': 25,
            'message': 'Extracting video information...'
        })
        
        # Quality settings optimized for free tier
        quality_settings = {
            'standard': {'format': 'bestaudio[abr<=128]', 'quality': '128'},
            'high': {'format': 'bestaudio[abr<=192]', 'quality': '192'},
            'ultra_high': {'format': 'bestaudio', 'quality': '256'}
        }
        
        settings = quality_settings.get(quality, quality_settings['high'])
        
        ydl_opts = {
            'format': settings['format'],
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': settings['quality'],
            }],
            'postprocessor_args': [
                '-ar', '44100',
                '-ac', '2',
                '-b:a', f"{settings['quality']}k",
                '-threads', '1',  # Reduced for free tier
                '-preset', 'fast',  # Changed from medium to fast
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            'concurrent_fragment_downloads': 1,  # Reduced for free tier
            'http_chunk_size': 512000,  # Reduced for free tier
            'buffer_size': 8192,  # Reduced for free tier
            'no_color': True,
            'quiet': False,
            'socket_timeout': 30,
            'fragment_retries': 2,
            'retries': 2,
            'extractor_retries': 1,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': True,  # Get metadata
            'writethumbnail': False,
        }
        
        # Add proxy only if enabled
        if USE_PROXY:
            proxy_url = get_working_proxy()
            if proxy_url:
                ydl_opts['proxy'] = proxy_url
        
        download_status[job_id].update({
            'progress': 40,
            'message': 'Starting download...'
        })
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Get info first
            info = ydl.extract_info(youtube_url, download=False)
            
            # Store metadata
            download_metadata[job_id] = {
                'title': info.get('title', 'Unknown Title'),
                'duration': info.get('duration', 0),
                'uploader': info.get('uploader', 'Unknown'),
                'upload_date': info.get('upload_date'),
                'view_count': info.get('view_count', 0)
            }
            
            download_status[job_id].update({
                'progress': 60,
                'message': f'Downloading: {info.get("title", "Unknown")[:50]}...',
                'video_title': info.get('title', 'Unknown'),
                'duration': info.get('duration', 0)
            })
            
            # Download
            ydl.download([youtube_url])
            
            download_status[job_id].update({
                'progress': 90,
                'message': 'Processing audio file...'
            })
            
            # Find downloaded files
            for pattern in ['*.mp3', '*.m4a', '*.webm']:
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
            error_code = "RATE_LIMITED_BY_YOUTUBE"
            message = "Rate limited by YouTube. Try again later."
        elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted"]):
            error_code = "VIDEO_UNAVAILABLE"
            message = "Video is unavailable, private, or deleted"
        else:
            error_code = "DOWNLOAD_FAILED"
            message = f"Download failed: {str(e)}"
        
        download_status[job_id].update({
            'status': 'failed',
            'progress': 0,
            'message': message,
            'error_code': error_code,
            'failed_at': time.time()
        })
    
    finally:
        active_downloads -= 1
        if temp_dir and job_id not in download_files:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

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

@app.route('/api/v1/download/status/<job_id>', methods=['GET'])
def check_download_status_ios(job_id):
    """iOS-friendly status check with detailed information"""
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
        
        if status['status'] == 'processing' and 'started_at' in status:
            processing_time = current_time - status['started_at']
            status['timing']['processing_seconds'] = int(processing_time)
    
    # Add metadata if available
    if job_id in download_metadata:
        status['video_metadata'] = download_metadata[job_id]
    
    # Add file information if completed
    if status['status'] == 'completed' and job_id in download_files:
        file_path = download_files[job_id]
        if os.path.exists(file_path):
            status['file_ready'] = True
            status['download_url'] = f"/api/v1/download/file/{job_id}"
        else:
            status['file_ready'] = False
            status['message'] = "File no longer available"
    
    return jsonify({
        "success": True,
        "data": status
    })

@app.route('/api/v1/download/file/<job_id>', methods=['GET'])
def get_download_file_ios(job_id):
    """iOS-optimized file download with proper headers"""
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
    
    # Create iOS-friendly filename
    metadata = download_metadata.get(job_id, {})
    title = metadata.get('title', 'audio')
    safe_title = re.sub(r'[^\w\s-]', '', title)[:50]
    quality = status.get('quality', 'high')
    safe_filename = f"{safe_title}_{quality}.mp3"
    
    # Get file info
    file_size = os.path.getsize(file_path)
    
    # Schedule cleanup after download
    def delayed_cleanup():
        time.sleep(120)  # Wait 2 minutes
        cleanup_download(job_id, silent=True)
    
    cleanup_thread = threading.Thread(target=delayed_cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    
    # Create response with iOS-friendly headers
    response = send_file(
        file_path,
        as_attachment=True,
        download_name=safe_filename,
        mimetype='audio/mpeg'
    )
    
    # Add metadata headers for iOS
    response.headers['X-File-Size'] = str(file_size)
    response.headers['X-File-Size-MB'] = str(round(file_size / (1024 * 1024), 2))
    response.headers['X-Video-Title'] = metadata.get('title', 'Unknown')
    response.headers['X-Video-Duration'] = str(metadata.get('duration', 0))
    response.headers['X-Video-Uploader'] = metadata.get('uploader', 'Unknown')
    response.headers['X-Quality'] = quality
    response.headers['X-Job-ID'] = job_id
    response.headers['Content-Disposition'] = f'attachment; filename="{safe_filename}"'
    
    return response

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
            
        if job_id in download_metadata:
            del download_metadata[job_id]
            
        if not silent:
            logger.info(f"Cleaned up download {job_id}")
    except Exception as e:
        if not silent:
            logger.error(f"Cleanup error for {job_id}: {e}")

# Legacy endpoints for backward compatibility
@app.route('/download/audio/ultrafast', methods=['POST'])
@rate_limit()
def legacy_ultrafast():
    """Legacy endpoint - redirects to new API"""
    return download_audio_quick()

@app.route('/download/audio/async', methods=['POST'])
@rate_limit()
def legacy_async():
    """Legacy endpoint - redirects to new API"""
    return download_audio_background()

@app.route('/download/status/<job_id>', methods=['GET'])
def legacy_status(job_id):
    """Legacy status endpoint"""
    return check_download_status_ios(job_id)

@app.route('/download/file/<job_id>', methods=['GET'])
def legacy_file(job_id):
    """Legacy file endpoint"""
    return get_download_file_ios(job_id)

# Utility endpoints for iOS
@app.route('/api/v1/server/stats', methods=['GET'])
def server_stats():
    """Get current server statistics"""
    try:
        memory_percent = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/tmp')
        free_gb = disk_usage.free / (1024**3)
        
        return jsonify({
            "success": True,
            "data": {
                "active_downloads": active_downloads,
                "queued_jobs": len([s for s in download_status.values() if s['status'] == 'queued']),
                "processing_jobs": len([s for s in download_status.values() if s['status'] == 'processing']),
                "completed_jobs": len([s for s in download_status.values() if s['status'] == 'completed']),
                "failed_jobs": len([s for s in download_status.values() if s['status'] == 'failed']),
                "memory_usage_percent": round(memory_percent, 1),
                "free_disk_gb": round(free_gb, 2),
                "proxy_active": current_proxy is not None,
                "proxy_enabled": USE_PROXY,
                "server_uptime": int(time.time() - psutil.boot_time()) if hasattr(psutil, 'boot_time') else 0
            }
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": {
                "code": "STATS_ERROR",
                "message": str(e)
            }
        }), 500

@app.route('/api/v1/download/cancel/<job_id>', methods=['DELETE'])
def cancel_download(job_id):
    """Cancel a download job"""
    if job_id not in download_status:
        return jsonify({
            "success": False,
            "error": {
                "code": "JOB_NOT_FOUND",
                "message": "Job not found"
            }
        }), 404
    
    status = download_status[job_id]['status']
    if status in ['completed', 'failed']:
        return jsonify({
            "success": False,
            "error": {
                "code": "CANNOT_CANCEL",
                "message": f"Cannot cancel job with status: {status}"
            }
        }), 400
    
    # Mark as cancelled
    download_status[job_id].update({
        'status': 'cancelled',
        'progress': 0,
        'message': 'Download cancelled by user',
        'cancelled_at': time.time()
    })
    
    # Schedule cleanup
    cleanup_timer = threading.Timer(10, cleanup_download, args=(job_id,))
    cleanup_timer.daemon = True
    cleanup_timer.start()
    
    return jsonify({
        "success": True,
        "data": {
            "job_id": job_id,
            "status": "cancelled",
            "message": "Download cancelled successfully"
        }
    }), 200

def periodic_cleanup():
    """Run periodic cleanup every 10 minutes"""
    while True:
        try:
            time.sleep(600)  # 10 minutes
            cleanup_old_downloads()
            gc.collect()
            
            # Log current status
            active_jobs = len(download_status)
            if active_jobs > 0:
                logger.info(f"Periodic cleanup completed. Active jobs: {active_jobs}")
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {e}")
    return jsonify({
        "success": False,
        "error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred"
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

@app.errorhandler(429)
def handle_rate_limit(e):
    return jsonify({
        "success": False,
        "error": {
            "code": "RATE_LIMIT_EXCEEDED",
            "message": "Too many requests. Please try again later.",
            "retry_after": 300
        }
    }), 429

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    
    # Free tier optimizations
    os.environ['FFMPEG_THREADS'] = '1'  # Reduced from 2 to 1
    os.environ['MALLOC_ARENA_MAX'] = '1'
    os.environ['PYTHONHASHSEED'] = '0'
    os.environ['OMP_NUM_THREADS'] = '1'
    
    # Aggressive startup cleanup for free tier
    logger.info("Performing startup cleanup...")
    aggressive_cleanup()
    
    # Start background cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()
    
    # CRITICAL: Skip proxy initialization on Render to prevent deployment timeout
    if os.environ.get('RENDER'):
        logger.info("Render deployment detected - skipping proxy initialization for faster startup")
        current_proxy = None
    else:
        # Only fetch proxy for local development
        logger.info("Local development detected - fetching initial proxy list...")
        try:
            if USE_PROXY:
                get_working_proxy()
        except Exception as e:
            logger.warning(f"Initial proxy fetch failed: {e}")
    
    # System checks (make these non-blocking and fast)
    try:
        import subprocess
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=5, text=True)
        if result.returncode == 0:
            logger.info("FFmpeg is available")
        else:
            logger.warning("FFmpeg check returned non-zero exit code")
    except subprocess.TimeoutExpired:
        logger.warning("FFmpeg check timed out")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FFmpeg not found - downloads may fail")
    except Exception as e:
        logger.warning(f"FFmpeg check failed: {e}")
    
    try:
        logger.info(f"yt-dlp version: {yt_dlp.version.__version__}")
    except Exception as e:
        logger.warning(f"Could not determine yt-dlp version: {e}")
    
    logger.info("=" * 60)
    logger.info("YouTube Audio Downloader Server v2.0-iOS-Render")
    logger.info("Optimized for Render.com FREE TIER")
    logger.info("Enhanced for iOS App Integration")
    logger.info("=" * 60)
    
    logger.info("API Endpoints:")
    logger.info("  • GET  /                           - Health check")
    logger.info("  • POST /api/v1/video/info          - Get video info (no download)")
    logger.info("  • POST /api/v1/download/quick      - Quick download (ultra-fast)")
    logger.info("  • POST /api/v1/download/background - Background download with progress")
    logger.info("  • GET  /api/v1/download/status/{id} - Check download status")
    logger.info("  • GET  /api/v1/download/file/{id}   - Download completed file")
    logger.info("  • DEL  /api/v1/download/cancel/{id} - Cancel download")
    logger.info("  • GET  /api/v1/server/stats         - Server statistics")
    
    logger.info("Features:")
    logger.info("  • iOS-optimized JSON responses")
    logger.info("  • CORS enabled for iOS apps")
    logger.info("  • Enhanced error handling with error codes")
    logger.info("  • Video metadata extraction")
    logger.info("  • Multiple quality options")
    logger.info("  • Progress tracking for background downloads")
    logger.info("  • Smart rate limiting")
    logger.info("  • Automatic cleanup")
    logger.info("  • Memory and disk monitoring")
    logger.info(f"  • Proxy support: {'ENABLED' if USE_PROXY else 'DISABLED'}")
    
    if USE_PROXY and current_proxy:
        logger.info(f"Proxy active: {current_proxy}")
    elif USE_PROXY:
        logger.info("Proxy enabled but none active (will fetch on first use)")
    else:
        logger.info("Direct connection (proxy disabled)")
    
    if os.environ.get('RENDER'):
        logger.info("Running on Render.com with free tier optimizations")
    
    logger.info("=" * 60)
    logger.info("Server starting...")
    
    try:
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        raise
