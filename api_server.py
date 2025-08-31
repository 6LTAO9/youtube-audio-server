import os
import tempfile
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
            
            # Add a check to confirm the cookies file exists
            cookie_path = os.path.join(os.getcwd(), 'cookies.txt')
            if not os.path.exists(cookie_path):
                print(f"Error: Cookies file not found at {cookie_path}")
                # We can still proceed with the download, but expect it to fail
                # if YouTube requires authentication.
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
            else:
                ydl_opts = {
                    'force_single_video': True,
                    'format': 'bestaudio/best',
                    'outtmpl': os.path.join(temp_dir, 'downloaded_audio.%(ext)s'),
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192',
                    }],
                    # The server is getting a "bot" error and needs to be logged in.
                    'cookiefile': cookie_path,
                }
            
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
                    # Send the file to the app.
                    return send_file(final_path, as_attachment=True)
                else:
                    return "Download failed on the server. File not found.", 500
            
            except Exception as e:
                print(f"An error occurred: {e}")
                return f"An error occurred: {e}", 500
            
    return "Invalid request format. Must be JSON.", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
