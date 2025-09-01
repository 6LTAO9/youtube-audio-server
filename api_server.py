import os
import tempfile
import time
import random
import threading
import uuid
from flask import Flask, request, send_file, jsonify, Response
import yt_dlp
import json
from pathlib import Path
import logging

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Render.com specific configurations
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size

# Store for tracking downloads
download_status = {}
download_files = {}

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint for Render.com"""
    return jsonify({"status": "healthy", "service": "youtube-audio-downloader"}), 200

@app.route('/download/audio/fast', methods=['POST'])
def download_audio_fast():
    """Fast download with high quality audio"""
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # Strip the playlist part from the URL if it exists
    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    # Clean URL further
    if "?" in youtube_url and "&" in youtube_url:
        base_url = youtube_url.split("?")[0]
        params = youtube_url.split("?")[1]
        if "v=" in params:
            video_id = params.split("v=")[1].split("&")[0]
            youtube_url = f"{base_url}?v={video_id}"

    logger.info(f"High-quality fast download for URL: {youtube_url}")

    # Create a temporary directory
    try:
        if os.path.exists('/tmp') and os.access('/tmp', os.W_OK):
            temp_dir = tempfile.mkdtemp(dir='/tmp')
        else:
            temp_dir = tempfile.mkdtemp(dir='.')
    except Exception as e:
        logger.error(f"Could not create temp directory: {e}")
        return jsonify({"error": "Server storage issue"}), 500
    
    try:
        # Get paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        
        # Get proxy from environment
        proxy_url = os.environ.get('HTTP_PROXY')

        # High-quality but optimized yt-dlp options
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio',  # Prefer high-quality formats
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',  # Highest MP3 quality
            }],
            'postprocessor_args': [
                '-ar', '48000',  # High sample rate
                '-ac', '2',      # Stereo
                '-b:a', '320k',  # High bitrate
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            'socket_timeout': 25,
            'fragment_retries': 2,
            'retries': 2,
            'no_warnings': True,
        }

        # Add cookies and proxy if available
        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
        if proxy_url:
            ydl_opts['proxy'] = proxy_url

        # Single attempt for speed
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                logger.info("Starting fast download...")
                ydl.download([youtube_url])
                
                # Find the downloaded file quickly
                for pattern in ['*.mp3', '*.m4a', '*.webm']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        logger.info(f"Fast download successful: {file_path}")
                        
                        safe_filename = f"audio_hq_{int(time.time())}.mp3"
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                
                return jsonify({"error": "Download completed but no audio file was created"}), 500
        
        except Exception as e:
            error_str = str(e).lower()
            logger.error(f"Fast download failed: {e}")
            
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({"error": "Rate limited. Try again in a few minutes."}), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted"]):
                return jsonify({"error": "Video is unavailable or private"}), 400
            else:
                return jsonify({"error": f"Download failed: {str(e)}"}), 500
    
    finally:
        # Clean up
        try:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

@app.route('/download/audio/async', methods=['POST'])
def download_audio_async():
    """Start async download and return job ID"""
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # Generate unique job ID
    job_id = str(uuid.uuid4())
    
    # Initialize status
    download_status[job_id] = {
        'status': 'started',
        'progress': 0,
        'message': 'Download started'
    }
    
    # Start download in background thread
    thread = threading.Thread(target=background_download, args=(job_id, youtube_url))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "started",
        "check_url": f"/download/status/{job_id}",
        "download_url": f"/download/file/{job_id}"
    }), 202

@app.route('/download/status/<job_id>', methods=['GET'])
def check_download_status(job_id):
    """Check the status of an async download"""
    if job_id not in download_status:
        return jsonify({"error": "Job not found"}), 404
    
    return jsonify(download_status[job_id])

@app.route('/download/file/<job_id>', methods=['GET'])
def get_download_file(job_id):
    """Get the downloaded file"""
    if job_id not in download_status:
        return jsonify({"error": "Job not found"}), 404
    
    status = download_status[job_id]
    if status['status'] != 'completed':
        return jsonify({"error": "Download not ready", "status": status['status']}), 400
    
    if job_id not in download_files:
        return jsonify({"error": "File not found"}), 404
    
    file_path = download_files[job_id]
    if not os.path.exists(file_path):
        return jsonify({"error": "File no longer available"}), 404
    
    return send_file(
        file_path,
        as_attachment=True,
        download_name=f"audio_hq_{job_id[:8]}.mp3",
        mimetype='audio/mpeg'
    )

def background_download(job_id, youtube_url):
    """Background download function"""
    try:
        # Update status
        download_status[job_id] = {
            'status': 'processing',
            'progress': 10,
            'message': 'Processing video URL'
        }
        
        # Clean URL
        if "&list=" in youtube_url:
            youtube_url = youtube_url.split("&list=")[0]
        
        # Create temp directory
        try:
            if os.path.exists('/tmp') and os.access('/tmp', os.W_OK):
                temp_dir = tempfile.mkdtemp(dir='/tmp')
            else:
                temp_dir = tempfile.mkdtemp(dir='.')
        except Exception as e:
            download_status[job_id] = {
                'status': 'failed',
                'progress': 0,
                'message': f'Storage error: {str(e)}'
            }
            return
        
        download_status[job_id]['progress'] = 25
        download_status[job_id]['message'] = 'Downloading audio...'
        
        # Download configuration
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        proxy_url = os.environ.get('HTTP_PROXY')
        
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio',  # Best quality available
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',  # Highest MP3 quality
            }],
            'postprocessor_args': [
                '-ar', '48000',  # High sample rate
                '-ac', '2',      # Stereo
                '-b:a', '320k',  # High bitrate
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            'socket_timeout': 30,
            'fragment_retries': 2,
            'retries': 2,
        }
        
        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
        if proxy_url:
            ydl_opts['proxy'] = proxy_url
        
        download_status[job_id]['progress'] = 50
        download_status[job_id]['message'] = 'Converting to MP3...'
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([youtube_url])
            
            # Find downloaded file
            for pattern in ['*.mp3', '*.m4a', '*.webm']:
                found_files = list(Path(temp_dir).glob(pattern))
                if found_files:
                    file_path = str(found_files[0])
                    download_files[job_id] = file_path
                    
                    download_status[job_id] = {
                        'status': 'completed',
                        'progress': 100,
                        'message': 'Download completed successfully'
                    }
                    
                    # Clean up after 1 hour
                    threading.Timer(3600, cleanup_download, args=(job_id,)).start()
                    return
            
            # No file found
            download_status[job_id] = {
                'status': 'failed',
                'progress': 0,
                'message': 'No audio file was created'
            }
    
    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            message = "Rate limited by YouTube. Try again later."
        elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted"]):
            message = "Video is unavailable or private"
        else:
            message = f"Download failed: {str(e)}"
        
        download_status[job_id] = {
            'status': 'failed',
            'progress': 0,
            'message': message
        }
        
        # Clean up temp directory
        try:
            import shutil
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

def cleanup_download(job_id):
    """Clean up download files and status after timeout"""
    try:
        if job_id in download_files:
            file_path = download_files[job_id]
            if os.path.exists(file_path):
                os.remove(file_path)
            del download_files[job_id]
        
        if job_id in download_status:
            del download_status[job_id]
            
        logger.info(f"Cleaned up download {job_id}")
    except Exception as e:
        logger.error(f"Cleanup error for {job_id}: {e}")

# Keep the original endpoint for compatibility
@app.route('/download/audio', methods=['POST'])
def download_audio():
    """Original endpoint - redirects to async download for reliability"""
    return download_audio_async()

@app.route('/download/audio/lossless', methods=['POST']) 
def download_audio_lossless():
    """Download in original format (no conversion) for best quality"""
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided"}), 400

    # Generate unique job ID for lossless download
    job_id = str(uuid.uuid4())
    
    download_status[job_id] = {
        'status': 'started',
        'progress': 0,
        'message': 'Starting lossless download'
    }
    
    # Start lossless download in background
    thread = threading.Thread(target=background_lossless_download, args=(job_id, youtube_url))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "started", 
        "message": "Lossless download started (no compression)",
        "check_url": f"/download/status/{job_id}",
        "download_url": f"/download/file/{job_id}"
    }), 202

def background_lossless_download(job_id, youtube_url):
    """Download original audio without any compression"""
    try:
        download_status[job_id] = {
            'status': 'processing',
            'progress': 10,
            'message': 'Getting best quality audio...'
        }
        
        if "&list=" in youtube_url:
            youtube_url = youtube_url.split("&list=")[0]
        
        try:
            if os.path.exists('/tmp') and os.access('/tmp', os.W_OK):
                temp_dir = tempfile.mkdtemp(dir='/tmp')
            else:
                temp_dir = tempfile.mkdtemp(dir='.')
        except Exception as e:
            download_status[job_id] = {
                'status': 'failed',
                'progress': 0,
                'message': f'Storage error: {str(e)}'
            }
            return
        
        download_status[job_id]['progress'] = 30
        download_status[job_id]['message'] = 'Downloading original audio...'
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        proxy_url = os.environ.get('HTTP_PROXY')
        
        # Lossless download - no conversion
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio',  # Keep original format
            'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
            # No postprocessors = no quality loss
            'prefer_ffmpeg': True,
            'noplaylist': True,
            'socket_timeout': 45,
            'fragment_retries': 3,
            'retries': 3,
        }
        
        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
        if proxy_url:
            ydl_opts['proxy'] = proxy_url
        
        download_status[job_id]['progress'] = 70
        download_status[job_id]['message'] = 'Processing lossless audio...'
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([youtube_url])
            
            # Find downloaded file (could be m4a, webm, etc.)
            for pattern in ['*.m4a', '*.webm', '*.opus', '*.mp3']:
                found_files = list(Path(temp_dir).glob(pattern))
                if found_files:
                    file_path = str(found_files[0])
                    download_files[job_id] = file_path
                    
                    # Get the actual format
                    file_ext = Path(file_path).suffix
                    
                    download_status[job_id] = {
                        'status': 'completed',
                        'progress': 100,
                        'message': f'Lossless download completed ({file_ext} format)'
                    }
                    
                    threading.Timer(3600, cleanup_download, args=(job_id,)).start()
                    return
            
            download_status[job_id] = {
                'status': 'failed',
                'progress': 0,
                'message': 'No audio file was created'
            }
    
    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg:
            message = "Rate limited. Try again later."
        elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted"]):
            message = "Video unavailable or private"
        else:
            message = f"Download failed: {str(e)}"
        
        download_status[job_id] = {
            'status': 'failed',
            'progress': 0,
            'message': message
        }
        
        try:
            import shutil
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {e}")
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    # Get port from environment variable (Render.com requirement)
    port = int(os.environ.get('PORT', 8080))
    
    # Check if ffmpeg is available
    import subprocess
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
        logger.info("FFmpeg is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("FFmpeg not found. This will cause failures on Render.com")
    
    # Check yt-dlp version
    try:
        logger.info(f"yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.info("Could not determine yt-dlp version")
    
    # Production-ready server settings
    if os.environ.get('RENDER'):
        logger.info("Running on Render.com")
    else:
        app.run(host='0.0.0.0', port=port, debug=True)
