import html2text
from bs4 import BeautifulSoup
import re
import os
import sys

# Get the input HTML file from the command line arguments
if len(sys.argv) != 2:
    # If no input file is provided, iterate through all HTML files in the current directory
    html_files = [f for f in os.listdir('.') if f.endswith('.html')]
else:
    html_files = [sys.argv[1]]

# Set up output directories
output_base_dir = os.path.join(os.getcwd(), 'outputs')
unified_markdown_dir = os.path.join(output_base_dir, 'unified_markdowns')
os.makedirs(unified_markdown_dir, exist_ok=True)

# Process each HTML file
for input_html_file in html_files:
    # Load the HTML file
    with open(input_html_file, 'r', encoding='utf-8') as file:
        html_content = file.read()

    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Initialize html2text parser
    html2text_handler = html2text.HTML2Text()
    html2text_handler.ignore_links = False  # Keep links
    html2text_handler.body_width = 0  # Avoid wrapping
    html2text_handler.ignore_images = True  # Ignore images

    # Base URL for links
    page_base_url = "https://he.wikisource.org/wiki/%D7%97%D7%95%D7%A7-%D7%99%D7%A1%D7%95%D7%93:_%D7%94%D7%9B%D7%A0%D7%A1%D7%AA"
    # Extract the source name from the page head title
    source_name = soup.title.string.strip() if soup.title else "Unknown Source"

    # Set output directory for clauses
    clauses_output_directory = os.path.join(output_base_dir, source_name)
    os.makedirs(clauses_output_directory, exist_ok=True)

    # Function to handle extraction based on 'law-cleaner1' markers
    def extract_clauses(soup):
        clauses = []
        clauses_markers = soup.find_all('div', class_='law-cleaner1')
        
        for marker in clauses_markers:
            clause_parts = []
            next_sibling = marker.find_next_sibling()

            # Iterate through the siblings until the next 'law-cleaner1' marker is found
            while next_sibling and (not next_sibling.has_attr('class') or 'law-cleaner1' not in next_sibling.get('class', [])):
                if next_sibling.name == 'div':
                    content = next_sibling.get_text(strip=True)
                    if content:
                        clause_parts.append(content)
                next_sibling = next_sibling.find_next_sibling()

            if clause_parts:
                clauses.append('\n'.join(clause_parts).strip())
        
        return clauses

    # Function to post-process the raw clauses and apply proper formatting
    def format_clauses(raw_clauses):
        formatted_lines = []
        individual_clauses = {}
        subclause_patterns = [re.compile(r'^\((\d+)\)'), re.compile(r'^\(([^\d])\)'), re.compile(r'^([א-ת])\.')]  # Patterns for subclauses
        subsubclause_patterns = [re.compile(r'^\d+\.'), re.compile(r'^\((\d+)\)')]  # Patterns for sub-subclauses

        # Flag to identify 'תוספות' section
        addendum_started = False
        addendum_content = []

        for clause in raw_clauses:
            lines = clause.split('\n')
            formatted_clause = []
            clause_number = ""
            clause_title = ""
            is_main_clause = True

            for line in lines:
                line = line.strip()
                # Check if we are in the 'תוספות' section
                if "תוספות" in line and not addendum_started:
                    addendum_started = True

                if addendum_started:
                    addendum_content.append(line)
                    continue

                is_subclause = any(pattern.match(line) for pattern in subclause_patterns)
                is_subsubclause = any(pattern.match(line) for pattern in subsubclause_patterns)

                if is_main_clause:
                    clause_number = line.split()[0]
                    clause_title = line[len(clause_number):].strip()
                    formatted_clause.append(f'**{source_name}: {clause_title}**\n\n')
                    link_line = f'[מקור: סעיף {clause_number} {clause_title}]({page_base_url}#%D7%A1%D7%A2%D7%99%D7%A3_{clause_number[:-1]})'
                    formatted_clause.append(f'{link_line}\n')
                    is_main_clause = False
                elif is_subclause:
                    formatted_clause.append(f'\n{line}\n')
                elif is_subsubclause:
                    formatted_clause.append(f'  - {line}\n')
                else:
                    formatted_clause.append(f'{line}\n')

            if formatted_clause:
                clause_content = ''.join(formatted_clause).strip()
                formatted_lines.append(clause_content)
                formatted_lines.append('\n---\n')

                if clause_number:
                    clean_clause_number = clause_number.rstrip('.')
                    clause_filename = os.path.join(clauses_output_directory, f'{source_name}_{clean_clause_number}.md')
                    if clause_filename not in individual_clauses:  # Avoid overwriting files
                        individual_clauses[clause_filename] = clause_content

        # Handle 'תוספות' section separately
        if addendum_content:
            addendum_text = '\n'.join(addendum_content).strip()
            addendum_filename = os.path.join(clauses_output_directory, f'{source_name}_תוספות.md')
            if addendum_filename not in individual_clauses:  # Ensure no overwriting
                individual_clauses[addendum_filename] = addendum_text
            formatted_lines.append(addendum_text)
            formatted_lines.append('\n---\n')

        return '\n'.join(formatted_lines), individual_clauses

    # Extract raw clauses
    raw_clauses = extract_clauses(soup)

    # Format the clauses
    markdown_content, individual_clauses = format_clauses(raw_clauses)

    # Write the output to a unified markdown file
    unified_md_file = os.path.join(unified_markdown_dir, f'{source_name}_output.md')
    with open(unified_md_file, 'w', encoding='utf-8') as md_file:
        md_file.write(markdown_content)

    # Split the unified markdown file into individual clause files
    for clause_filename, clause_content in individual_clauses.items():
        if not os.path.exists(clause_filename):  # Avoid overwriting existing files
            with open(clause_filename, 'w', encoding='utf-8') as clause_file:
                clause_file.write(clause_content)

    print(f"Markdown extraction complete! Output saved in {unified_md_file} and individual clause files saved in '{clauses_output_directory}' directory.")