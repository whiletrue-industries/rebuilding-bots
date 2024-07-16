import codecs
import os
import dotenv
import requests
import yaml
import json
from openai import OpenAI

dotenv.load_dotenv('benchmark/.env')

import dataflows as DF
import dataflows_airtable as DFA

AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']

def get_config():
    ret = DF.Flow(
        DFA.load_from_airtable('appiOFTgaF4f0ls0j', 'Configuration', 'Grid view', apikey=AIRTABLE_API_KEY),
        DF.checkpoint('config'),
        DF.printer()
    ).results()[0][0]
    ret = {row['key']: row['value'] for row in ret if row.get('key')}
    return ret

def get_response_from_openai():
    api_key = os.environ['OPENAI_API_KEY']
    # Create openai client and get completion for prompt with the 'gpt4-o' model:
    client = OpenAI(api_key=api_key)
 
    def func(rows):
        for row in rows:
            question = row['question']
            prompt = row['prompt']
            if prompt.startswith('ERROR: '):
                row['success'] = False
                row['score'] = 0
                row['observation'] = prompt
                yield row
                continue
            completion = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {'role': 'assistant', 'content': prompt}
                ]
            )
            response = completion.choices[0].message.content
            if '{' in response[:15]:
                response = '{' + response.split('{', 1)[1]
            if '}' in response[-15:]:
                response = response.rsplit('}', 1)[0] + '}'
            with open(f'logs/{question}.response.json', 'w') as f:
                f.write(response)
            response = json.loads(response)
            row.update(response)
            yield row

    return DF.Flow(
        DF.add_field('score', 'number'),
        DF.add_field('success', 'boolean'),
        DF.add_field('observation', 'string'),
        func
    )

def get_budget_prompt(config, row):
    prompt = config['evaluate budget question prompt']
    context = dict()
    instructions = [
        ('question', row['question']),
        ('answer', row['answer']),
        ('context', context),
    ]
    if row.get('possible answer'):
        context['reference_answer'] = row['reference answer']
    elif row.get('sql'):
        sql = row['sql']
        sql = codecs.encode(codecs.encode(sql, 'utf-8'), 'base64').decode('ascii').replace('\n', '')
        resp = requests.get('https://next.obudget.org/api/query', params={'query': sql}).json()
        if 'rows' in resp:
            rows = resp['rows']
        else:
            return 'ERROR: ' + str(resp)
        print('Got {} rows for {}'.format(len(rows), row['sql']))
        # assert len(rows) > 0, 'No rows returned from query {}'.format(row['sql'])
        context['data'] = rows
    for k, v in instructions:
        prompt = prompt + yaml.dump({k: v}, allow_unicode=True) + '\n'
    with open(f'logs/{row['question']}.prompt.txt', 'w') as f:
        f.write(prompt)
    return prompt

def fetch_answer(config, row):
    question = row['question']
    api_key = config['dify_api_key']
    request = dict(
        inputs=dict(),
        query=question,
        response_mode='streaming',
        conversation_id='',
        user='benchmark',        
    )
    response = requests.post('https://api.dify.ai/v1/chat-messages', json=request, headers={'Authorization': 'Bearer ' + api_key})
    answer = ''
    if response.status_code == 200:
        with open(f'logs/{question}.ndjson', 'w') as f:
            for line in response.iter_lines():
                if line and line.startswith(b'data: '):
                    line = line.decode('utf-8')[6:]
                    event = json.loads(line)
                    if event.get('event') == 'agent_message':
                        answer += event['answer']
                    else:
                        json.dump(event, f, ensure_ascii=False)
                        f.write('\n')
    return answer

def run_benchmark(table, config, row_filter, prompter):
    print('Running benchmark...')
    DF.Flow(
        DFA.load_from_airtable('appiOFTgaF4f0ls0j', table, 'Grid view', apikey=AIRTABLE_API_KEY),
        DF.update_resource(-1, name='benchmark'),
        DF.filter_rows(lambda row: row.get('success') != 'Passed'),
        DF.filter_rows(row_filter),
        DF.add_field('answer', 'string', lambda row: fetch_answer(config, row)),
        DF.checkpoint('answers'),
        DF.add_field('prompt', 'string', lambda row: prompter(config, row)),
        get_response_from_openai(),
        DF.select_fields([DFA.AIRTABLE_ID_FIELD, 'answer', 'score', 'success', 'observation']),
        DF.rename_fields({'answer': 'actual answer'}),
        DF.set_type('success', type='string', transform=lambda v: 'Error' if v is None else ('Passed' if v else 'Failed')),
        DF.set_type('score', type='integer', transform=lambda v: int(v)),
        DF.printer(),
        DFA.dump_to_airtable({
            ('appiOFTgaF4f0ls0j', table): {
                'resource-name': 'benchmark',
            }
        }, apikey=AIRTABLE_API_KEY),
    ).process()

if __name__ == '__main__':
    config = get_config()
    run_benchmark(
        'BUDGET QA',
        dict(dify_api_key=os.environ['DIFY_API_KEY_BUDGET'], **config),
        lambda row: row.get('question') and (row.get('possible answer') or row.get('sql')),
        get_budget_prompt,
    )
