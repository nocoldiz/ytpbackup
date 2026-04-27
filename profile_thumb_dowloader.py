import os
import json
import requests
from bs4 import BeautifulSoup

INPUT_FILE = "./docs/ytpoopers_index.json"
OUTPUT_FOLDER = "./docs/profile_thumbnails"

def main():
    # 1. Create the output directory if it doesn't exist
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created folder: {OUTPUT_FOLDER}/")

    # 2. Read the JSON file
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            youtubers_data = json.load(f)
    except FileNotFoundError:
        print(f" [!] Error: The file '{INPUT_FILE}' was not found.")
        print(f"     Please make sure the 'docs' folder exists in your current directory.")
        return
    except json.JSONDecodeError:
        print(f" [!] Error: The file '{INPUT_FILE}' is not a valid JSON file.")
        return

    # Add a standard User-Agent so YouTube doesn't block the request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    # Flag to check if we actually need to save changes later
    changes_made = False

    # 3. Loop through the JSON data
    for url, info in youtubers_data.items():
        channel_name = info.get("channel_name", "UnknownChannel")
        print(f"Processing {channel_name}...")
        
        try:
            # Fetch the YouTube channel page
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Parse the HTML to find the profile picture URL
            soup = BeautifulSoup(response.text, "html.parser")
            meta_tag = soup.find("meta", property="og:image")
            
            if meta_tag and meta_tag.get("content"):
                img_url = meta_tag["content"]
                
                # Download the image
                img_data = requests.get(img_url, headers=headers).content
                
                # Define safe filename
                safe_name = "".join(c for c in channel_name if c.isalnum() or c in " _-").strip()
                filename = f"{safe_name}.jpg"
                file_path = os.path.join(OUTPUT_FOLDER, filename)
                
                # Save the image to the folder
                with open(file_path, "wb") as f:
                    f.write(img_data)
                    
                # UPDATE THE DICTIONARY
                info["thumbnail"] = filename
                changes_made = True
                    
                print(f" [+] Saved profile picture to {file_path}")
            else:
                print(f" [-] Could not find profile picture for {channel_name}")
                
        except requests.exceptions.RequestException as e:
            print(f" [!] Network error processing {channel_name}: {e}")
        except Exception as e:
            print(f" [!] Unexpected error processing {channel_name}: {e}")

    # 4. Save the updated dictionary back to the JSON file
    if changes_made:
        print(f"\nUpdating {INPUT_FILE} with new thumbnail filenames...")
        try:
            with open(INPUT_FILE, "w", encoding="utf-8") as f:
                # json.dump overwrites the file with nicely indented formatting
                json.dump(youtubers_data, f, indent=2, ensure_ascii=False)
            print(" [+] JSON file updated successfully.")
        except Exception as e:
            print(f" [!] Error saving back to '{INPUT_FILE}': {e}")
    else:
        print("\nNo thumbnails were downloaded, so the JSON file was not modified.")

if __name__ == "__main__":
    main()