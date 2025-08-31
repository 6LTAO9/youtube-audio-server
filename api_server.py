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

            # Use an absolute path to the cookies file to prevent path issues on the server.
            # We assume 'cookies.txt' is in the same directory as this script.
            script_dir = os.path.dirname(os.path.abspath(__file__))
            cookie_path = os.path.join(script_dir, 'cookies.txt')

            print(f"Checking for cookies file at: {cookie_path}")

            ydl_opts = {
                'force_single_video': True,
                'format': 'bestaudio/best',
                'outtmpl': os.path.join(temp_dir, 'downloaded_audio.%(ext)s'),
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                # Add a random delay between requests to avoid rate-limiting (429 errors).
                'sleep_requests': True,
                'sleep_interval': (5, 10), # Increased sleep interval for better rate-limiting avoidance
                'max_sleep_interval': 60, # The maximum sleep interval in seconds
                # Enable retries with exponential backoff for network-related errors.
                'retries': 15,
                # Set a common user agent to bypass bot detection.
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            }
            if os.path.exists(cookie_path):
                print("Cookies file found. Using it for authentication.")
                ydl_opts['cookiefile'] = cookie_path
            else:
                print(f"Error: Cookies file not found at {cookie_path}. Proceeding without it.")

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
