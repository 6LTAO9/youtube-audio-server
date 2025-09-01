import os
import tempfile
import time
import random
from flask import Flask, request, send_file, jsonify
import yt_dlp
import json
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/download/audio', methods=['POST'])
def download_audio():
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
        # Keep only the 'v' parameter
        if "v=" in params:
            video_id = params.split("v=")[1].split("&")[0]
            youtube_url = f"{base_url}?v={video_id}"

    logger.info(f"Processing URL: {youtube_url}")

    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Get paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')
        
        logger.info(f"Checking for cookies file at: {cookie_path}")
        
        # Get proxy from environment
        proxy_url = os.environ.get('HTTP_PROXY')
        if proxy_url:
            logger.info(f"Using proxy: {proxy_url}")

        # Enhanced yt-dlp options - minimal config to avoid tuple comparison bug
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio',
            'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            
            # Minimal retry settings to avoid type comparison issues
            'retries': 10,
            'fragment_retries': 10,
            'skip_unavailable_fragments': True,
            
            # Headers and user agent
            'http_headers': {
                'User-Agent': random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
                ]),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            },
            
            # Network settings
            'socket_timeout': 30,
            'nocheckcertificate': True,
            'prefer_free_formats': True,
            'no_warnings': False,
            'extractaudio': True,
            'audioformat': 'mp3',
            'embed_subs': False,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'ignoreerrors': False,
        }

        # Add cookies if available
        if os.path.exists(cookie_path):
            logger.info("Using cookies file for authentication")
            ydl_opts['cookiefile'] = cookie_path
        else:
            logger.info("No cookies file found, proceeding without authentication")

        # Add proxy if available
        if proxy_url:
            ydl_opts['proxy'] = proxy_url

        # Add random initial delay
        initial_delay = random.uniform(1, 5)
        logger.info(f"Initial delay: {initial_delay:.2f} seconds")
        time.sleep(initial_delay)

        max_attempts = 3
        
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Download attempt {attempt}/{max_attempts}")
            
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    # Extract info first to validate URL
                    info = ydl.extract_info(youtube_url, download=False)
                    if not info:
                        return jsonify({"error": "Could not extract video information"}), 400
                    
                    # Check if video is available
                    if info.get('availability') in ['private', 'premium_only', 'subscriber_only']:
                        return jsonify({"error": f"Video is not available: {info.get('availability')}"}), 400
                    
                    # Now download
                    logger.info(f"Downloading: {info.get('title', 'Unknown')}")
                    ydl.download([youtube_url])
                    
                    # Find the downloaded file
                    downloaded_files = list(Path(temp_dir).glob('*.mp3'))
                    if not downloaded_files:
                        # Check for other audio formats
                        downloaded_files = list(Path(temp_dir).glob('*.m4a'))
                        if not downloaded_files:
                            downloaded_files = list(Path(temp_dir).glob('*.webm'))
                    
                    if downloaded_files:
                        file_path = str(downloaded_files[0])
                        logger.info(f"Download successful: {file_path}")
                        
                        # Get original filename for download
                        original_name = info.get('title', 'audio') + '.mp3'
                        # Clean filename for download
                        safe_filename = "".join(c for c in original_name if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                    else:
                        logger.error(f"No audio file found after download attempt {attempt}")
                        if attempt < max_attempts:
                            time.sleep(random.uniform(10, 20))
                            continue
                
            except yt_dlp.utils.DownloadError as e:
                error_msg = str(e).lower()
                logger.error(f"yt-dlp error on attempt {attempt}: {e}")
                
                if "429" in error_msg or "too many requests" in error_msg:
                    if attempt < max_attempts:
                        delay = min(180, 30 * (2 ** (attempt - 1)) + random.uniform(0, 30))
                        logger.info(f"Rate limited. Waiting {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue
                    else:
                        return jsonify({"error": "Rate limited by YouTube. Please try again later."}), 429
                
                elif any(phrase in error_msg for phrase in ["unavailable", "private", "deleted", "removed"]):
                    return jsonify({"error": "Video is unavailable, private, or has been removed"}), 400
                
                elif "sign in" in error_msg or "age" in error_msg:
                    return jsonify({"error": "Video requires age verification or sign-in"}), 400
                
                elif "copyright" in error_msg or "blocked" in error_msg:
                    return jsonify({"error": "Video is blocked due to copyright restrictions"}), 400
                
                else:
                    if attempt < max_attempts:
                        delay = random.uniform(15, 30)
                        logger.info(f"Download error, retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue
                    else:
                        return jsonify({"error": f"Download failed: {str(e)}"}), 500
            
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}: {e}")
                if attempt < max_attempts:
                    time.sleep(random.uniform(10, 20))
                    continue
                else:
                    return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
        
        return jsonify({"error": "Download failed after all attempts"}), 500
    
    finally:
        # Clean up temporary directory
        try:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Could not clean up temp directory: {e}")


@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Unhandled error: {e}")
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    # Check if ffmpeg is available
    import subprocess
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
        logger.info("FFmpeg is available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.warning("FFmpeg not found. Audio conversion may not work properly.")
    
    app.run(host='0.0.0.0', port=8080, debug=False)
