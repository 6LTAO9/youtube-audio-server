import os
import tempfile
from flask import Flask, request, send_file
import yt_dlp
import json

app = Flask(__name__)

@app.route('/download/audio', methods=['POST'])
def download_audio():
    if request.is_json:
        data = request.json
        youtube_url = data.get('url')
        
        if not youtube_url:
            return "No URL provided", 400

        if "&list=" in youtube_url:
            youtube_url = youtube_url.split("&list=")[0]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            proxy_url = os.getenv('http://bqgkebje:u67be8bb5ia9@23.95.150.145:6114/')
            username = os.getenv('u6517763368@gmail.com ')
            password = os.getenv('yttomp3.,?')

            ydl_opts = {
                'force_single_video': True,
                'format': 'bestaudio/best',
                'outtmpl': os.path.join(temp_dir, 'downloaded_audio.%(ext)s'),
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                'proxy': proxy_url
            }

            if username and password:
                ydl_opts['username'] = username
                ydl_opts['password'] = password

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(youtube_url, download=True)
                    final_path = ydl.prepare_filename(info)
                    
                if final_path.endswith('.webm') or final_path.endswith('.ogg'):
                    final_path = final_path.rsplit('.', 1)[0] + '.mp3'

                if os.path.exists(final_path):
                    return send_file(final_path, as_attachment=True)
                else:
                    return "Download failed on the server. File not found.", 500
            
            except Exception as e:
                print(f"An error occurred: {e}")
                return f"An error occurred: {e}", 500
            
    return "Invalid request format. Must be JSON.", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
