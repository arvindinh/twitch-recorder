import sys
import yt_dlp

def download_twitch_vod(url):
    """
    Downloads a Twitch VOD at the highest quality and formats it as an MP4.
    """
    
    # --- CHANGE MADE HERE ---
    # Define the directory on your large block storage volume
    save_path = '/mnt/volume_sfo3_01/'

    # Configure yt-dlp options
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        
        # --- CHANGE MADE HERE ---
        # Prepend the save_path to the output template
        'outtmpl': f'{save_path}%(uploader)s_%(title)s_%(id)s.%(ext)s',
        
        # Ensures the final output is mp4 for easy YouTube uploading
        'merge_output_format': 'mp4',
        # Suppress massive console spam
        'quiet': False,
        'no_warnings': True,
    }

    print(f"Starting download for: {url}")
    print(f"Saving to: {save_path}")
    print("This might take a while depending on the VOD length...")

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # yt-dlp expects a list of URLs
            ydl.download([url])
            print("\nDownload completed successfully!")

    except yt_dlp.utils.DownloadError as e:
        print(f"\nFailed to download VOD: {e}")
    except KeyboardInterrupt:
        print("\nDownload cancelled by user.")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure a URL was passed in the terminal
    if len(sys.argv) != 2:
        print("Usage: python download_vod.py <twitch_vod_url>")
        sys.exit(1)

    target_url = sys.argv[1]
    download_twitch_vod(target_url)