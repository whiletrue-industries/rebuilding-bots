import json
import yaml
import os
import logging
import sys
from bs4 import BeautifulSoup
import re

# Constants
MAX_CHUNK_LENGTH = 100000
WS = re.compile(r'\s+', re.UNICODE | re.MULTILINE)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- PART 1: HTML Parsing and Structure Extraction ---
def clean(text):
    """Clean up whitespace in the text."""
    return WS.sub(' ', text).strip()

def clean_el(el):
    """Extract and clean text from an HTML element."""
    text = el.get_text(strip=True).split('[תיקון')[0]
    return clean(text)

def parse_html_to_structure(html_content):
    """Parse HTML content to extract the document structure."""
    soup = BeautifulSoup(html_content, 'html.parser')
    document_structure = []
    clause, part, chapters, clauses, subclauses = None, None, None, None, None

    for el in soup.find_all(['div', 'h2', 'h1', 'h3'], recursive=True):
        cls = el.attrs.get('class') or []

        if 'law-part' in cls:
            title = clean_el(el)
            number, title = title.split(':', 1)
            part = {"number": number, "title": clean(title), "chapters": []}
            chapters = part['chapters']
            document_structure.append(part)

        elif 'law-section' in cls and chapters is not None:
            number, title = clean_el(el).split(':', 1)
            chapter = {"number": number, "title": clean(title), "clauses": []}
            clauses = chapter['clauses']
            chapters.append(chapter)

        elif 'law-number' in cls and clauses is not None:
            clause = {"number": clean_el(el).rstrip('.'), "title": None, "text": None, "subclauses": []}
            clauses.append(clause)
            subclauses = clause['subclauses']

        elif 'law-desc' in cls and clause is not None:
            clause['title'] = clean_el(el)

        elif 'law-content' in cls and clause is not None:
            clause['text'] = clean_el(el)

        elif 'law-number1' in cls and subclauses is not None:
            subclause = {"number": clean_el(el), "text": None, "sub_subclauses": []}
            subclauses.append(subclause)
            sub_subclauses = subclause['sub_subclauses']

        elif 'law-content1' in cls and subclauses is not None:
            subclause['text'] = clean_el(el)

        elif 'law-number2' in cls and subclauses is not None:
            sub_subclause = {"number": clean_el(el), "text": None}
            subclauses[-1]['sub_subclauses'].append(sub_subclause)

        elif 'law-content3' in cls and subclauses is not None:
            subclauses[-1]['sub_subclauses'][-1]['text'] = clean_el(el)

    return document_structure

# --- PART 2: Saving Data in JSON and YAML ---
def save_json(data, output_file):
    """Save data to a JSON file."""
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists
    try:
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        logging.info(f"JSON file saved: {output_file}")
    except Exception as e:
        logging.error(f"Failed to save JSON: {e}")

def save_yaml(data, output_file):
    """Save data to a YAML file."""
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists
    try:
        with open(output_file, 'w', encoding='utf-8') as yaml_file:
            yaml.dump(data, yaml_file, allow_unicode=True, sort_keys=False)
        logging.info(f"YAML file saved: {output_file}")
    except Exception as e:
        logging.error(f"Failed to save YAML: {e}")

# --- PART 3: Markdown Generation ---
def format_path_part(part):
    """Format the part or chapter title/number."""
    if part.get('number') and part.get('title'):
        result = f'{part["number"]} - {part["title"]}'
    else:
        return None
    if part.get('subtitle'):
        result += f' ({part["subtitle"]})'
    return result

def all_clauses(structure):
    """Yield all clauses with their path (part and chapter)."""
    for part in structure:
        for chapter in part.get("chapters", []):
            path = [part, chapter]
            for clause in chapter.get("clauses", []):
                yield path, clause

def generate_markdown_content_for_clause(path, clause, MAX_CHUNK_LENGTH):
    """Generate markdown content for a single clause."""
    markdown_content = []
    subclauses = clause.get('subclauses', []) + [None]
    cont = False

    while subclauses:
        content = []
        path_ = list(filter(None, (format_path_part(p) for p in path))) + [f'סעיף {clause["number"]}']
        title = clause.get('title', '')
        text = clause.get('text', '')
        cont_text = ' (המשך)' if cont else ''
        content.append(f'**{title}{cont_text}**\n')
        content.append(f'[מקור: {" / ".join(path_)}](https://he.wikisource.org/wiki/תקנון_הכנסת#סעיף_{clause["number"]})\n')

        if text:
            content.append(text)

        subclauses_content, subclauses = process_subclauses(subclauses, content, MAX_CHUNK_LENGTH)
        markdown_content.append('\n'.join(subclauses_content))

    return '\n'.join(markdown_content)

def process_subclauses(subclauses, content, MAX_CHUNK_LENGTH):
    """Process subclauses and handle chunking if necessary."""
    markdown_content = []
    while subclauses:
        sc = subclauses.pop(0)
        if sc is None:
            break
        sc_content = [f' * {sc["number"]} {sc["text"] or ""}']
        
        ssc_content = []
        for ssc in sc.get('sub_subclauses', []):
            ssc_content.append(f'   * {ssc["number"]} {ssc["text"]}')
        
        if len('\n'.join(content + sc_content + ssc_content)) > MAX_CHUNK_LENGTH:
            return markdown_content, [sc] + subclauses
        
        content += sc_content + ssc_content
        markdown_content.append('\n'.join(content))
    
    return markdown_content, subclauses

def process_document_structure(structure, output_dir, MAX_CHUNK_LENGTH):
    """Process the entire document structure and save markdown files."""
    for path, clause in all_clauses(structure):
        if not clause.get('text') and not clause.get('subclauses'):
            continue
        
        clause_content = generate_markdown_content_for_clause(path, clause, MAX_CHUNK_LENGTH)
        filename = f'{clause["number"]}.md'
        save_markdown_file(clause_content, output_dir, filename)

def save_markdown_file(content, output_dir, filename):
    """Save the markdown content to a file."""
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, filename)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"File saved: {file_path}")
    except Exception as e:
        logging.error(f"Failed to write file {file_path}: {e}")

# --- Main Program ---
def main(html_file, output_json, output_yaml, output_md_dir):
    """Main function to process the HTML, save JSON/YAML, and generate Markdown files."""
    try:
        # Step 1: Read HTML and parse structure
        with open(html_file, 'r', encoding='utf-8') as file:
            html_content = file.read()
        document_structure = parse_html_to_structure(html_content)

        # Step 2: Save document structure as JSON and YAML
        save_json(document_structure, output_json)
        save_yaml(document_structure, output_yaml)

        # Step 3: Generate Markdown files
        process_document_structure(document_structure, output_md_dir, MAX_CHUNK_LENGTH)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        logging.error("Usage: python process_clauses.py <input_html> <output_json> <output_yaml> <output_md_dir>")
        sys.exit(1)

    html_file = sys.argv[1]
    output_json = sys.argv[2]
    output_yaml = sys.argv[3]
    output_md_dir = sys.argv[4]

    main(html_file, output_json, output_yaml, output_md_dir)
