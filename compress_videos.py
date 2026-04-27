import os
import subprocess
from pathlib import Path

# --- CONFIGURATION ---
VIDEO_DIR = Path("./videos")

# FFmpeg settings for H.265 (HEVC)
# -crf 20: Visually lossless quality for H.265
# -preset slower: Spends more time on compression to get the smallest file size
FFMPEG_CMD = [
    "ffmpeg", "-y", "-i", "{input}",
    "-c:v", "libx265", "-crf", "20", "-preset", "slower",
    "-c:a", "copy",  # Keep original audio to preserve quality
    "-tag:v", "hvc1", # Ensures compatibility with Apple/QuickTime
    "{output}"
]

# Supported video extensions
EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".ts"}

def get_size(file_path):
    return file_path.stat().st_size

def convert_to_h265():
    if not VIDEO_DIR.exists():
        print(f"Error: Folder {VIDEO_DIR} does not exist.")
        return

    # Find all videos in the directory and subdirectories
    video_files = [f for f in VIDEO_DIR.rglob("*") if f.suffix.lower() in EXTENSIONS]
    
    print(f"Found {len(video_files)} videos. Starting H.265 conversion...\n")

    for vid in video_files:
        # Define a temporary output file
        temp_output = vid.with_suffix(".temp_h265.mp4")
        
        print(f"Processing: {vid.name}")
        
        # Build the command string
        cmd = [arg.format(input=str(vid), output=str(temp_output)) for arg in FFMPEG_CMD]
        
        try:
            # Execute conversion (hiding output unless there is an error)
            subprocess.run(cmd, check=True, capture_output=True)
            
            if temp_output.exists():
                orig_size = get_size(vid)
                new_size = get_size(temp_output)
                
                # Check if the new file is actually smaller
                if new_size < orig_size:
                    reduction = (orig_size - new_size) / orig_size * 100
                    print(f"  [SUCCESS] Smaller! {orig_size/1024/1024:.1f}MB -> {new_size/1024/1024:.1f}MB (-{reduction:.1f}%)")
                    
                    # Target final name (converting extension to .mp4)
                    final_name = vid.with_suffix(".mp4")
                    
                    # Delete original
                    vid.unlink() 
                    
                    # If the final name already existed (e.g. converting .mkv to .mp4), 
                    # rename handles the overwrite.
                    temp_output.rename(final_name)
                else:
                    print(f"  [SKIP] New file was larger ({new_size/1024/1024:.1f}MB). Keeping original.")
                    temp_output.unlink()
                    
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] FFmpeg failed on {vid.name}. Leaving original untouched.")
            if temp_output.exists():
                temp_output.unlink()
        except Exception as e:
            print(f"  [ERROR] Unexpected error: {e}")

    print("\n--- All H.265 conversions complete ---")

if __name__ == "__main__":
    convert_to_h265()