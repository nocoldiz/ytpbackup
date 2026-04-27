import json
import re
import os

def tag_languages():
    input_path = os.path.join('docs', 'video_index.json')
    output_path = os.path.join('docs', 'video_index.json')

    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    print(f"Loading {input_path}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Known channel mappings
    known_channels = {
        "italian": [
            "https://www.youtube.com/@mrpoldoakbar2849",
            "https://www.youtube.com/@TottiBest92",
            "https://www.youtube.com/@despotaaa",
            "https://www.youtube.com/@bassman85x",
            "https://www.youtube.com/@ZioTok83",
            "https://www.youtube.com/@tracFelix96trac",
        ],
        "english": [
            "https://www.youtube.com/@cs188",
            "https://www.youtube.com/@KroboProductions",
            "https://www.youtube.com/@EmperorLemon",
            "https://www.youtube.com/@Deepercutt",
            "https://www.youtube.com/@Hurricoaster",
            "https://www.youtube.com/@DaThings",
        ],
        "spanish": [
            "https://www.youtube.com/@ParodiadorAnimado",
            "https://www.youtube.com/@HDLuigi",
            "https://www.youtube.com/@Catdany",
            "https://www.youtube.com/@NinterYT",
            "https://www.youtube.com/@Reloxard",
        ],
        "german": [
            "https://www.youtube.com/@PetersKotstube",
            "https://www.youtube.com/@Sostrator",
            "https://www.youtube.com/@YTKFactory",
            "https://www.youtube.com/@FanboyAllianz",
            "https://www.youtube.com/@MinerMorsel",
        ],
        "french": [],
        "russian": []
    }

    # Define regex patterns
    patterns = {
        "spanish": [
            r'YTPH|Chavo\s+del\s+8|Loquendo|Pelea\s+de\s+invalidos|Vete\s+a\s+la\s+Versh|Pooppa[ñn]ol'
        ],
        "french": [
            r'YTPFR|YTP\s+FR|Brocante|Joueur\s+du\s+Grenier|JDG|Koh\s+Lanta|Denis\s+Brogniart|David\s+Goodenough'
        ],
        "german": [
            r'YouTube\s+Kacke|Marcell\s+D\'Avis|Peter\s+Zwegat|Kinski|Löwenzahn|Peter\s+Lustig|1&1'
        ],
        "russian": [
            r'RYTP|РУТП|Поцык|Повар|Сашко|Гамаз|Пенек'
        ],
        "italian": [
            r'matteo\s+montesi|avventure|Zeb|Collegio|Bigazzi|Soccer|Ganon|Billy\s+Mays|Branduardi|Luigi|Ambrogio|Risotto|ariete|Harry\s+potter|Round|Peppa|Grylls|Tennis|Acid|Favij|Testoh|Pingu',
            r'Dipr[eè]|Bello\s+Figo|Germano|Grillo|Gesù|Nabbo|Yotobi|Berlusconi|Muniz|Travaglio|Nemesis|Testo|Papa|Super\s+Quark|Iscritti|YTM|YTG|MLG|YTK',
            r'Sentence\s+Mix|Ear\s?rape|G-Major|Mondo\s+emo|Pubblicità|Spot|Spongebob|Reverse|Masking|Pitch\s+Shift',
            r'Mosconi|Benson|Brumotti|Master\s?chef|Mister\s+Lui|Pappalardo|Sgarbi|Razzi|Salvini|Renzi|Rio\s+mare|Gerry\s+Scotti|Fazio',
            r'Kabu|Nocoldiz|Poldo|Cloroformio|Giannino|Gianni\s+Morandi|Doraemon|Me\s+cont[ro]o\s+Te'
        ]
    }

    # Compile patterns
    compiled_patterns = {}
    for lang, p_list in patterns.items():
        combined = '|'.join(p_list)
        compiled_patterns[lang] = re.compile(combined, re.IGNORECASE)

    count = 0
    tagged_counts = {lang: 0 for lang in patterns}
    tagged_counts["english"] = 0 # Track english too
    
    # Track channels by language
    channels_by_lang = {lang: set(urls) for lang, urls in known_channels.items()}

    for video_id, video in data.items():
        title = video.get('title')
        thread_titles = video.get('thread_titles', [])
        channel_url = video.get('channel_url')
        
        matched_lang = None
        
        # 1. Check known channels first
        if channel_url:
            for lang, urls in known_channels.items():
                if channel_url in urls:
                    matched_lang = lang
                    break
        
        # 2. If not matched, try keyword matching
        if not matched_lang:
            search_text = []
            if title:
                search_text.append(title)
            if thread_titles:
                search_text.extend(thread_titles)
            
            full_text = " ".join(search_text)
            
            if full_text:
                for lang, regex in compiled_patterns.items():
                    if regex.search(full_text):
                        matched_lang = lang
                        break
        
        if matched_lang:
            video['language'] = matched_lang
            if matched_lang not in tagged_counts:
                tagged_counts[matched_lang] = 0
            tagged_counts[matched_lang] += 1
            count += 1
            
            # Collect the channel URL if it's available
            if channel_url:
                channels_by_lang[matched_lang].add(channel_url)

    print(f"Finished tagging. Total videos updated: {count}")
    for lang, c in sorted(tagged_counts.items()):
        print(f"  - {lang}: {c}")

    print(f"Saving to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Export channels to txt file
    channels_file = 'channels_by_language.txt'
    print(f"Exporting channels to {channels_file}...")
    with open(channels_file, 'w', encoding='utf-8') as f:
        for lang, urls in sorted(channels_by_lang.items()):
            var_name = f"{lang.upper()}_CHANNELS"
            f.write(f"{var_name} = [\n")
            for url in sorted(list(urls)):
                f.write(f"    \"{url}\",\n")
            f.write("]\n\n")

if __name__ == "__main__":
    tag_languages()
