import json
import os
import glob

# Constants
VIDEO_INDEX = "docs/video_index.json"
SOURCES_INDEX = "docs/sources_index.json"
VIDEOS_DIR = "docs/videos"
SOURCES_DIR = "docs/sources"

# Tags to remove (case-insensitive)
TAGS_TO_REMOVE = {"youtube", "ytp", "youtubepoop", "tube", "you", "yt", "poop"}

def clean_entry(data):
    """
    Cleans a single video/source entry object:
    5. Remove specific blacklisted tags.
    """
    # 5. Remove blacklisted tags
    if 'tags' in data and isinstance(data['tags'], list):
        data['tags'] = [t for t in data['tags'] if t.lower() not in TAGS_TO_REMOVE]
        
    return data

def process_index_file(file_path):
    """
    Processes a main index file (dictionary of entries).
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    print(f"Processing index: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            index = json.load(f)
        
        print(f"  Cleaning {len(index)} entries...")
        for key in index:
            index[key] = clean_entry(index[key])
            
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(index, f, separators=(',', ':'), ensure_ascii=False)
        print(f"  Successfully saved {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def process_directory(dir_path):
    """
    Processes all JSON files in a directory (each file is one entry).
    """
    if not os.path.exists(dir_path):
        print(f"Directory not found: {dir_path}")
        return
    
    print(f"Processing directory: {dir_path}")
    json_files = glob.glob(os.path.join(dir_path, "*.json"))
    count = 0
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            cleaned_data = clean_entry(data)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, separators=(',', ':'), ensure_ascii=False)
            count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"  Processed {count} files in {dir_path}")

if __name__ == "__main__":
    print("Starting cleanup script...")
    process_index_file(VIDEO_INDEX)
    process_index_file(SOURCES_INDEX)
    process_directory(VIDEOS_DIR)
    process_directory(SOURCES_DIR)
    print("Cleanup complete.")
