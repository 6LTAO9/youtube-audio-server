import os
import tempfile
import time
import random
from flask import Flask, request, send_file
import yt_dlp
import json

app = Flask(__name__)

# This route will handle the download request from your app.
@app.route('/download/audio', methods=['POST'])
def download_audio():
    # Make sure we're getting a JSON request.
    if request.is_json:
        data = request.json
        youtube_url = data.get('url')

        if not youtube_url:
            return "No URL provided", 400

        # Strip the playlist part from the URL if it exists
        if "&list=" in youtube_url:
            youtube_url = youtube_url.split("&list=")[0]

        # Use a temporary directory to store the file
        with tempfile.TemporaryDirectory() as temp_dir:

            # Use an absolute path to the cookies file to prevent path issues on the server.
            # We assume 'cookies.txt' is in the same directory as this script.
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cookie_path = os.path.join(script_dir, 'cookies.txt')

            print(f"Checking for cookies file at: {cookie_path}")

            # Get the proxy URL from the environment variable
            proxy_url = os.environ.get('HTTP_PROXY')
            print(f"Using proxy: {proxy_url}")

            # Enhanced yt-dlp options to better handle rate limiting
            ydl_opts = {
                'force_single_video': True,
                'format': 'bestaudio/best',
                'outtmpl': os.path.join(temp_dir, 'downloaded_audio.%(ext)s'),
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                # Enhanced sleep and retry settings
                'sleep_requests': True,
                'sleep_interval': (10, 20),  # Increased sleep interval
                'max_sleep_interval': 120,   # Longer max sleep
                'sleep_interval_subtitles': 15,
                # More aggressive retry settings
                'retries': 25,               # Increased retries
                'fragment_retries': 25,
                'skip_unavailable_fragments': True,
                # Enhanced user agent rotation
                'user_agent': random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
                ]),
                # Use proxy if available
                'proxy': proxy_url,
                # Enhanced geographic and network settings
                'geo_bypass': True,
                'geo_bypass_country': random.choice(['US', 'CA', 'GB', 'DE', 'FR']),
                # Additional headers to appear more browser-like
                'http_headers': {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                },
                # Timeout settings
                'socket_timeout': 30,
                # Don't check certificates (sometimes helps with proxies)
                'nocheckcertificate': True,
                # Prefer free formats to avoid premium checks
                'prefer_free_formats': True,
                # Use alternative extractors if main one fails
                'extract_flat': False,
                'ignoreerrors': False,
            }

            if os.path.exists(cookie_path):
                print("Cookies file found. Using it for authentication.")
                ydl_opts['cookiefile'] = cookie_path
            else:
                print(f"Cookies file not found at {cookie_path}. Proceeding without it.")

            # Add a random delay before starting the download (0-30 seconds)
            initial_delay = random.randint(0, 30)
            print(f"Adding initial delay of {initial_delay} seconds to avoid rate limiting...")
            time.sleep(initial_delay)

            max_attempts = 3
            attempt = 0
            
            while attempt < max_attempts:
                attempt += 1
                print(f"Download attempt {attempt}/{max_attempts}")
                
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(youtube_url, download=True)
                        # Get the final path from the download info, as this is the most reliable way.
                        final_path = ydl.prepare_filename(info)

                    # The filename can change during post-processing. Check for the mp3 extension.
                    if final_path.endswith('.webm') or final_path.endswith('.ogg'):
                        # The postprocessor created a new file, so we need to get that new path.
                        final_path = final_path.rsplit('.', 1)[0] + '.mp3'

                    # Check if the file was created successfully.
                    if os.path.exists(final_path):
                        print(f"Download successful on attempt {attempt}")
                        # Send the file to the app.
                        return send_file(final_path, as_attachment=True)
                    else:
                        print(f"File not found after attempt {attempt}")
                        if attempt < max_attempts:
                            retry_delay = random.randint(60, 120)
                            print(f"Waiting {retry_delay} seconds before retry...")
                            time.sleep(retry_delay)
                        continue

                except yt_dlp.utils.DownloadError as e:
                    error_str = str(e)
                    print(f"Download error on attempt {attempt}: {error_str}")
                    
                    if "HTTP Error 429" in error_str or "Too Many Requests" in error_str:
                        if attempt < max_attempts:
                            # Exponential backoff for 429 errors
                            retry_delay = min(300, 60 * (2 ** (attempt - 1)) + random.randint(0, 60))
                            print(f"Rate limited. Waiting {retry_delay} seconds before retry {attempt + 1}...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            print("Max attempts reached. YouTube is blocking requests.")
                            return "YouTube is temporarily blocking requests. Please try again later (5-10 minutes).", 429
                    
                    elif "Video unavailable" in error_str or "Private video" in error_str:
                        return f"Video is unavailable or private: {error_str}", 400
                    
                    elif "Sign in to confirm your age" in error_str:
                        return "This video requires age verification. Cannot download without authentication.", 400
                    
                    else:
                        if attempt < max_attempts:
                            retry_delay = random.randint(30, 60)
                            print(f"General download error. Waiting {retry_delay} seconds before retry...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            return f"Download failed after {max_attempts} attempts: {error_str}", 500

                except Exception as e:
                    error_str = str(e)
                    print(f"Unexpected error on attempt {attempt}: {error_str}")
                    
                    if attempt < max_attempts:
                        retry_delay = random.randint(30, 60)
                        print(f"Unexpected error. Waiting {retry_delay} seconds before retry...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        return f"Unexpected error after {max_attempts} attempts: {error_str}", 500

            return "Download failed after all retry attempts.", 500

    return "Invalid request format. Must be JSON.", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
