import io
import pathlib
import dataflows as DF


def collect_sources_files(config_dir, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = [(f.name, f.open('rb'), 'text/markdown') for f in files]
    return file_streams

def collect_sources_split(config_dir, context_name, source):
    filename = config_dir / source
    content = filename.read_text()
    content = content.split('\n---\n')
    file_streams = [io.BytesIO(c.strip().encode('utf-8')) for c in content]
    file_streams = [(f'{context_name}_{i}.md', f, 'text/markdown') for i, f in enumerate(file_streams)]
    return file_streams

def collect_sources_google_spreadsheet(context_name, source):
    resources, dp, _ = DF.Flow(
        DF.load(source, name='rows'),
    ).results()
    rows = resources[0]
    headers = [f.name for f in dp.resources[0].schema.fields]
    file_streams = []
    for idx, row in enumerate(rows):
        content = ''
        if len(headers) > 1:
            for i, header in enumerate(headers):
                if row.get(header):
                    if i > 0:
                        content += f'{header}:\n{row[header]}\n\n'
                    else:
                        content += f'{row[header]}\n\n'
        if content:
            file_streams.append((f'{context_name}_{idx}.md', io.BytesIO(content.strip().encode('utf-8')), 'text/markdown'))
    return file_streams

def collect_context_sources(context_, config_dir: pathlib.Path):
    context_name = context_['name']
    context_type = context_['type']
    if context_type == 'files':
        file_streams = collect_sources_files(config_dir, context_name, context_['source'])
    elif context_type == 'split':
        file_streams = collect_sources_split(config_dir, context_name, context_['source'])
    elif context_type == 'google-spreadsheet':
        file_streams = collect_sources_google_spreadsheet(context_name, context_['source'])
    else:
        raise ValueError(f'Unknown context type: {context_type}')
    return file_streams

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources