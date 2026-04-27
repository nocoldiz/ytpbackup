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
        # Combine multiple strings if necessary, ensuring no leading/trailing pipes
        combined = '|'.join(p_list)
        compiled_patterns[lang] = re.compile(combined, re.IGNORECASE)

    count = 0
    tagged_counts = {lang: 0 for lang in patterns}

    for video_id, video in data.items():
        title = video.get('title')
        thread_titles = video.get('thread_titles', [])
        
        # Collect all possible text to search in
        search_text = []
        if title:
            search_text.append(title)
        if thread_titles:
            search_text.extend(thread_titles)
        
        full_text = " ".join(search_text)
        
        if not full_text:
            continue

        matched_lang = None
        for lang, regex in compiled_patterns.items():
            if regex.search(full_text):
                matched_lang = lang
                break
        
        if matched_lang:
            video['language'] = matched_lang
            tagged_counts[matched_lang] += 1
            count += 1

    print(f"Finished tagging. Total videos updated: {count}")
    for lang, c in tagged_counts.items():
        print(f"  - {lang}: {c}")

    print(f"Saving to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    tag_languages()
