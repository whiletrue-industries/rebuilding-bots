import codecs
import os
import dotenv
import requests
import yaml
import json
from pathlib import Path
from openai import OpenAI
from openai.types.beta.threads.runs.run_step import ToolCallsStepDetails
import requests_openapi

dotenv.load_dotenv('benchmark/.env')

import dataflows as DF
import dataflows_airtable as DFA

TEMP = 0 # 0.00000001
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
                ],
                temperature=TEMP
            )
            response = completion.choices[0].message.content
            if '{' in response[:15]:
                response = '{' + response.split('{', 1)[1]
            if '}' in response[-15:]:
                response = response.rsplit('}', 1)[0] + '}'
            with open(f'logs/{question}.response.json', 'w') as f:
                f.write(response)
            try:
                response = json.loads(response)
            except Exception as e:
                response = {
                    'success': False,
                    'score': 0,
                    'observation': f'ERROR: Bad response: {response}\n\n{e}'
                }
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
    if row.get('reference answer'):
        context['reference_answer'] = row['reference answer']
    elif row.get('sql'):
        sql = row['sql']
        if sql.lower().strip() == 'empty':
            context['reference_answer'] = 'relevant data not available, the agent''s response should convey that an answer is not available'
        else:
            sql = codecs.encode(codecs.encode(sql, 'utf-8'), 'base64').decode('ascii').replace('\n', '')
            try:
                resp = requests.get('https://next.obudget.org/api/query', params={'query': sql}).json()
                if 'rows' in resp:
                    rows = resp['rows']
                else:
                    return f'ERROR: SQL query failed: {resp}'
            except Exception as e:
                return f'ERROR: SQL query failed2 {e}'
            print('Got {} rows for {}'.format(len(rows), row['sql']))
            # assert len(rows) > 0, 'No rows returned from query {}'.format(row['sql'])
            context['data'] = rows
    for k, v in instructions:
        prompt = prompt + yaml.dump({k: v}, allow_unicode=True) + '\n'
    with open(f'logs/{row["question"]}.prompt.txt', 'w') as f:
        f.write(prompt)
    return prompt


def get_takanon_prompt(config, row):
    prompt = config['evaluate takanon question prompt']
    instructions = [
        ('question', row['question']),
        ('expected answer', row['reference answer']),
        ('expected references', row['references']),
        ('expected citations', row['citations']),
        ('actual answer', row['answer']),
    ]
    for k, v in instructions:
        prompt = prompt + yaml.dump({k: v}, allow_unicode=True) + '\n'
    with open(f'logs/{row["question"]}.prompt.txt', 'w') as f:
        f.write(prompt)
    return prompt

def get_openapi_output(openapi_spec, tool_name, parameters):
    client = requests_openapi.Client(req_opts={"timeout": 30})
    client.load_spec_from_file(Path('specs/openapi') / openapi_spec)
    resp = getattr(client, tool_name)(**parameters)
    print('RESP URL', resp.url)
    # print('OUTPUT', resp.text[:200])
    if resp.status_code != 200:
        print(f'ERROR: {resp.status_code} {resp.text}')
        return {'error': resp.text}
    try:
        return resp.json()
    except Exception as e:
        print(f'ERROR: {e} {resp.text}')
        return {'error': resp.text}

def get_dataset_info_cache(arguments, output):
    dataset = arguments['dataset']
    path = Path('specs') / 'budgetkey' / 'dataset-info-cache' / f'{dataset}.yaml'
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            yaml.dump(output, f, allow_unicode=True, default_style='|')
    else:
        with open(path) as f:
            output = yaml.safe_load(f)
            print('USED CACHED', dataset)
    return output

def fetch_single_answer(row):
    question = row['question']
    assistant_id, openapi_spec = row['assistant_id'], row['openapi_spec']
    print('QUESTION:', question)
    api_key = os.environ['OPENAI_API_KEY']
    # Create openai client and get completion for prompt with the 'gpt4-o' model:
    client = OpenAI(api_key=api_key)
    
    for retry in range(3):
        try:
            notes = []
            step_ids = set()
            thread = client.beta.threads.create()
            message = client.beta.threads.messages.create(
                thread_id=thread.id,
                role='user',
                content=question
            )
            run = client.beta.threads.runs.create_and_poll(
                thread_id=thread.id,
                assistant_id=assistant_id,
                temperature=TEMP,
                top_p=1,
            )
            assert run.status in ['completed', 'requires_action']
            while True:
                print('RUN', run.id, run.status, run.temperature, run.top_p)
                tool_outputs = []
                notes.append(f'RUN {run.status}')
                for step in client.beta.threads.runs.steps.list(run.id, thread_id=thread.id, order='asc', extra_query=dict(include=['step_details.tool_calls[*].file_search.results[*].content'])):
                    if step.id in step_ids:
                        continue
                    step_ids.add(step.id)
                    if step.type == 'tool_calls':
                        step_details: ToolCallsStepDetails = step.step_details
                        for tool_call in step_details.tool_calls:
                            if tool_call.type == 'function':
                                print('TOOL', tool_call.id, tool_call.function.name, tool_call.function.arguments)
                                notes.append(f'{tool_call.function.name}({tool_call.function.arguments})')
                            elif tool_call.type == 'file_search':
                                print('FILE-SEARCH', tool_call.id, tool_call.file_search)
                                notes.append(f'file-search:')
                                for result in(tool_call.file_search.results or []):
                                    text = result.content[0].text if result.content else None
                                    if text:
                                        notes.append(f'>>\n{text}\n<<')

                if run.status == 'completed': 
                    break

                for tool in run.required_action.submit_tool_outputs.tool_calls:
                    arguments = json.loads(tool.function.arguments)
                    if tool.function.name == 'DatasetDBQuery':
                        arguments['page_size'] = 30
                    output = get_openapi_output(openapi_spec, tool.function.name, arguments)
                    if tool.function.name == 'DatasetInfo':
                        output = get_dataset_info_cache(arguments, output)
                    tool_outputs.append(dict(
                        tool_call_id=tool.id,
                        output=json.dumps(output, ensure_ascii=False, indent=2)
                    ))
                run = client.beta.threads.runs.submit_tool_outputs_and_poll(
                    thread_id=thread.id,
                    run_id=run.id,
                    tool_outputs=tool_outputs
                )
                assert run.status in ['completed', 'requires_action'], f'RUN STATUS: {run.status}'
            messages = client.beta.threads.messages.list(
                thread_id=thread.id,
                order='asc'
            )
            answer = []
            for message in messages:
                if message.role == 'assistant':
                    for content in message.content:
                        if content.type == 'text':
                            answer.append(content.text.value)
            row['answer'] = '\n'.join(answer)
            row['notes'] = '\n'.join(notes)
            return row
        except Exception as e:
            print('ERROR', f'retrying... {retry+1}/3', str(e))
            continue

def fetch_answer(agent_name, openapi_spec, concurrency):

    api_key = os.environ['OPENAI_API_KEY']
    # Create openai client and get completion for prompt with the 'gpt4-o' model:
    client = OpenAI(api_key=api_key)
    all_assistants = client.beta.assistants.list()
    assistant_id = [a.id for a in all_assistants if a.name == agent_name][0]

    return DF.Flow(
        DF.add_field('answer', 'string'),
        DF.add_field('notes', 'string'),
        DF.add_field('assistant_id', 'string', assistant_id),
        DF.add_field('openapi_spec', 'string', openapi_spec),
        DF.parallelize(fetch_single_answer, num_processors=concurrency),
        DF.delete_fields(['assistant_id', 'openapi_spec'])
    )

def run_benchmark(table, agent_name, openapi_spec, config, row_filter, prompter, local, reuse_answers, only_failed, specific_test, concurrency):
    print(f'Running benchmark for {agent_name} against {table}... select={specific_test}')
    DF.Flow(
        DFA.load_from_airtable('appiOFTgaF4f0ls0j', table, 'Grid view', apikey=AIRTABLE_API_KEY),
        DF.update_resource(-1, name='benchmark'),
        DF.filter_rows(lambda row: row.get('success') not in ('Passed', 'Suspended')) if only_failed else None,
        DF.filter_rows(lambda row: row[DFA.AIRTABLE_ID_FIELD] == specific_test) if specific_test else None,
        DF.filter_rows(row_filter),
        DF.printer(),
        fetch_answer(agent_name, openapi_spec, concurrency),
        DF.checkpoint(f'{table}-answers') if reuse_answers else None,
        DF.add_field('prompt', 'string', lambda row: prompter(config, row)),
        get_response_from_openai(),
        DF.select_fields([DFA.AIRTABLE_ID_FIELD, 'answer', 'notes', 'score', 'success', 'observation']),
        DF.rename_fields({'answer': 'actual answer'}),
        DF.set_type('success', type='string', transform=lambda v: 'Error' if v is None else ('Passed' if v else 'Failed')),
        DF.set_type('score', type='integer', transform=lambda v: int(v)),
        DF.dump_to_path('out/benchmarks/' + table)
        if local else
        DFA.dump_to_airtable({
            ('appiOFTgaF4f0ls0j', table): {
                'resource-name': 'benchmark',
            }
        }, apikey=AIRTABLE_API_KEY),
        DF.printer(),
    ).process()

def run_benchmarks(environment, bots, local, reuse_answers, select, concurrency):
    only_failed = select == 'failed'
    specific_test = None if select in ('all', 'failed') else select
    config = get_config()
    suffix = '' if environment == 'production' else ' - פיתוח'
    if bots in ('all', 'budgetkey'):
        run_benchmark(
            'BUDGET QA', 'בוט נתונים תקציביים' + suffix, 'budgetkey.yaml', config,
            lambda row: row.get('question') and (row.get('reference answer') or row.get('sql')),
            get_budget_prompt,
            local, reuse_answers, only_failed, specific_test, concurrency
        )
    if bots in ('all', 'takanon'):
        run_benchmark(
            'TAKANON QA', 'בוט תקנון הכנסת' + suffix, 'takanon.yaml', config,
            lambda row: row.get('reference answer') and row.get('references') and row.get('citations'),
            get_takanon_prompt,
            local, reuse_answers, only_failed, specific_test, concurrency
        )
    # print(get_openapi_output('budgetkey.yaml', 'DatasetInfo', {'dataset': 'supports_data'}).json())