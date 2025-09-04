@app.route('/download/audio/fast', methods=['POST'])
@rate_limit(max_requests=12, window=300)  # 12 requests per 5 minutes for main endpoint
def download_audio_fast():
    """Optimized fast download for iOS app"""
    global active_downloads
    
    # Quick resource check
    can_proceed, message = check_system_resources()
    if not can_proceed:
        logger.warning(f"Resource check failed: {message}")
        return jsonify({"error": message, "code": "RESOURCE_LIMIT"}), 503
    
    if not request.is_json:
        return jsonify({"error": "Request must be JSON", "code": "INVALID_FORMAT"}), 400
    
    data = request.json
    youtube_url = data.get('url')

    if not youtube_url:
        return jsonify({"error": "No URL provided", "code": "MISSING_URL"}), 400

    # Clean URL
    if "&list=" in youtube_url:
        youtube_url = youtube_url.split("&list=")[0]
    
    logger.info(f"Fast download request: {youtube_url}")
    
    active_downloads += 1
    temp_dir = None
    
    try:
        # Create temp directory
        temp_dir = tempfile.mkdtemp(dir='/tmp', prefix='yt_fast_')
        
        # Cookie handling
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_path = os.path.join(script_dir, 'cookies.txt')

        # Simple, reliable settings for iOS app
        ydl_opts = {
            'format': 'bestaudio[abr<=160]/bestaudio[ext=m4a]/bestaudio',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s'),
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '160',
            }],
            'postprocessor_args': [
                '-ar', '44100',
                '-ac', '2',
                '-b:a', '160k',
                '-threads', '2',
                '-preset', 'fast',
            ],
            'prefer_ffmpeg': True,
            'keepvideo': False,
            'noplaylist': True,
            
            # Network settings optimized for reliability
            'concurrent_fragment_downloads': 3,
            'http_chunk_size': 1048576,  # 1MB chunks
            'buffer_size': 32768,
            'no_color': True,
            'quiet': True,
            'no_warnings': True,
            
            # More forgiving retry settings
            'socket_timeout': 20,
            'fragment_retries': 2,
            'retries': 2,
            'extractor_retries': 1,
            
            # Skip unnecessary operations
            'writesubtitles': False,
            'writeautomaticsub': False,
            'embed_subs': False,
            'writeinfojson': False,
            'writethumbnail': False,
            'extract_flat': False,
        }

        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # FIXED: Extract video info first to get the title
                info = ydl.extract_info(youtube_url, download=False)
                video_title = info.get('title', 'Unknown Video')
                
                # Clean the title for filename use
                safe_title = "".join(c for c in video_title if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_title = safe_title[:50]  # Limit to 50 characters
                if not safe_title:
                    safe_title = "Downloaded Audio"
                
                # Download the video
                ydl.download([youtube_url])
                
                # Find and return file
                for pattern in ['*.mp3', '*.m4a', '*.webm', '*.opus']:
                    found_files = list(Path(temp_dir).glob(pattern))
                    if found_files:
                        file_path = str(found_files[0])
                        
                        # FIXED: Use actual video title for filename instead of timestamp
                        safe_filename = f"{safe_title}.mp3"
                        
                        file_size = os.path.getsize(file_path)
                        logger.info(f"Download successful: {file_size} bytes - '{video_title}'")
                        
                        def cleanup_after_send():
                            time.sleep(45)  # Wait longer before cleanup
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except Exception as e:
                                logger.error(f"Cleanup error: {e}")
                        
                        cleanup_thread = threading.Thread(target=cleanup_after_send)
                        cleanup_thread.daemon = True
                        cleanup_thread.start()
                        
                        return send_file(
                            file_path, 
                            as_attachment=True, 
                            download_name=safe_filename,
                            mimetype='audio/mpeg'
                        )
                
                return jsonify({
                    "error": "Download completed but no audio file was created",
                    "code": "NO_OUTPUT_FILE"
                }), 500
        
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle specific errors
            if "429" in error_str or "too many requests" in error_str:
                return jsonify({
                    "error": "YouTube rate limit exceeded. Please wait a few minutes.",
                    "code": "YOUTUBE_RATE_LIMIT"
                }), 429
            elif any(phrase in error_str for phrase in ["unavailable", "private", "deleted", "removed"]):
                return jsonify({
                    "error": "Video is unavailable, private, or has been removed",
                    "code": "VIDEO_UNAVAILABLE"
                }), 400
            elif any(phrase in error_str for phrase in ["not available in your country", "geo", "region", "blocked in your country"]):
                return jsonify({
                    "error": "Video not available in server region",
                    "code": "GEO_BLOCKED"
                }), 400
            elif "copyright" in error_str:
                return jsonify({
                    "error": "Video blocked due to copyright restrictions",
                    "code": "COPYRIGHT_BLOCKED"
                }), 400
            else:
                logger.error(f"Download failed: {e}")
                return jsonify({
                    "error": "Download failed due to server error",
                    "code": "DOWNLOAD_FAILED",
                    "details": str(e)[:200]  # Truncate long error messages
                }), 500
    
    finally:
        active_downloads -= 1
        # Cleanup temp directory if still exists
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")
