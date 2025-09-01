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

        # Minimal yt-dlp options to avoid tuple comparison bug
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'no_warnings': False,
            'extractaudio': True,
            'audioformat': 'mp3',
            'ignoreerrors': False,
            'noplaylist': True,
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

        max_attempts = 3
        
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Download attempt {attempt}/{max_attempts}")
            
            try:
                # Add random delay between attempts (but not using yt-dlp's sleep settings)
                if attempt > 1:
                    delay = random.uniform(5, 15)
                    logger.info(f"Waiting {delay:.1f} seconds before attempt {attempt}")
                    time.sleep(delay)
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    # Try to extract info first
                    try:
                        info = ydl.extract_info(youtube_url, download=False)
                        if not info:
                            logger.error("Could not extract video information")
                            if attempt < max_attempts:
                                continue
                            return jsonify({"error": "Could not extract video information"}), 400
                        
                        # Check availability
                        availability = info.get('availability')
                        if availability in ['private', 'premium_only', 'subscriber_only', 'needs_auth']:
                            return jsonify({"error": f"Video is not available: {availability}"}), 400
                        
                        logger.info(f"Video info extracted: {info.get('title', 'Unknown')}")
                        
                    except Exception as info_error:
                        logger.error(f"Info extraction failed: {info_error}")
                        if attempt < max_attempts:
                            continue
                        return jsonify({"error": f"Could not access video: {str(info_error)}"}), 400
                    
                    # Now try to download
                    logger.info("Starting download...")
                    try:
                        ydl.download([youtube_url])
                    except Exception as download_error:
                        logger.error(f"Download failed: {download_error}")
                        # Re-raise to be caught by outer exception handler
                        raise download_error
                    
                    # Find the downloaded file
                    downloaded_files = []
                    
                    # Look for various audio formats
                    for pattern in ['*.mp3', '*.m4a', '*.webm', '*.ogg']:
                        found_files = list(Path(temp_dir).glob(pattern))
                        downloaded_files.extend(found_files)
                    
                    if downloaded_files:
                        # Sort by modification time and get the newest file
                        downloaded_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                        file_path = str(downloaded_files[0])
                        logger.info(f"Download successful: {file_path}")
                        
                        # Get original filename for download
                        original_name = info.get('title', 'audio') + '.mp3'
                        # Clean filename for download
                        safe_filename = "".join(c for c in original_name if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
                        if not safe_filename.endswith('.mp3'):
                            safe_filename += '.mp3'
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                    else:
                        logger.error(f"No audio file found after download attempt {attempt}")
                        if attempt < max_attempts:
                            continue
                        return jsonify({"error": "Download completed but no audio file was created"}), 500
                
            except yt_dlp.utils.DownloadError as e:
                error_msg = str(e).lower()
                logger.error(f"yt-dlp DownloadError on attempt {attempt}: {e}")
                
                # Handle specific known errors
                if "429" in error_msg or "too many requests" in error_msg:
                    if attempt < max_attempts:
                        delay = min(120, 20 * attempt + random.uniform(0, 20))
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
                        delay = random.uniform(10, 20)
                        logger.info(f"Download error, retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue
                    else:
                        return jsonify({"error": f"Download failed: {str(e)}"}), 500
            
            except Exception as e:
                error_str = str(e)
                logger.error(f"Unexpected error on attempt {attempt}: {error_str}")
                
                # Check if this is the tuple comparison error
                if "'>' not supported between instances of 'int' and 'tuple'" in error_str:
                    logger.error("Detected tuple comparison bug in yt-dlp")
                    if attempt < max_attempts:
                        # Try with even more minimal options
                        logger.info("Retrying with minimal yt-dlp configuration...")
                        ydl_opts = {
                            'format': 'bestaudio',
                            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
                            'postprocessors': [{
                                'key': 'FFmpegExtractAudio',
                                'preferredcodec': 'mp3',
                            }],
                            'noplaylist': True,
                        }
                        if os.path.exists(cookie_path):
                            ydl_opts['cookiefile'] = cookie_path
                        if proxy_url:
                            ydl_opts['proxy'] = proxy_url
                        
                        time.sleep(random.uniform(5, 10))
                        continue
                    else:
                        return jsonify({"error": "yt-dlp internal error. Please try updating yt-dlp or try a different video."}), 500
                
                if attempt < max_attempts:
                    delay = random.uniform(10, 20)
                    logger.info(f"Unexpected error, retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
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
    
    # Check yt-dlp version
    try:
        logger.info(f"yt-dlp version: {yt_dlp.version.__version__}")
    except:
        logger.info("Could not determine yt-dlp version")
    
    app.run(host='0.0.0.0', port=8080, debug=False)
