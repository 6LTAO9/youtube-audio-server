import os
import tempfile
from flask import Flask, request, send_file
import yt_dlp
import json

app = Flask(__name__)

@app.route('/download/audio', methods=['POST'])
def download_audio():
    if not request.is_json:
        return "Invalid request format. Must be JSON.", 400

    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return "No URL provided", 400

    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    # Use a temp directory that will be deleted after the request
    with tempfile.TemporaryDirectory() as temp_dir:
        proxy_url = os.getenv('PROXY_URL')
        username = os.getenv('PROXY_USERNAME')
        password = os.getenv('PROXY_PASSWORD')

        ydl_opts = {
            'force_single_video': True,
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(temp_dir, 'downloaded_audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        }

        # Properly format the proxy string if credentials are provided
        if username and password and proxy_url:
            # Assumes the proxy URL is of the format http://some_address:port
            scheme, address = proxy_url.split('://', 1)
            ydl_opts['proxy'] = f'{scheme}://{username}:{password}@{address}'
        elif proxy_url:
            ydl_opts['proxy'] = proxy_url

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=True)
                final_path = info.get('_filename')
                if not final_path:
                    # Fallback in case the filename key is not found
                    # This is unlikely with yt-dlp, but good for robustness
                    final_path = os.path.join(temp_dir, 'downloaded_audio.mp3')
            
            if os.path.exists(final_path):
                # Use download_name to give a proper filename to the user
                download_name = info.get('title', 'downloaded_audio').replace(' ', '_') + '.mp3'
                return send_file(final_path, as_attachment=True, download_name=download_name)
            else:
                return "Download failed on the server. File not found.", 500
        
        except yt_dlp.utils.DownloadError as e:
            print(f"Download Error: {e}")
            return f"Download Error: {e}", 500
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return f"An unexpected error occurred: {e}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
