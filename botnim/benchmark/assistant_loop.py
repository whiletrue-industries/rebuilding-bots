import yaml
import json
from pathlib import Path
import requests_openapi
from typing import Dict
import re

from openai import OpenAI
from openai.types.beta.threads.runs.run_step import ToolCallsStepDetails

from botnim.query import QueryClient
from botnim.tools.elastic_vector_search import elastic_vector_search_handler
TEMP = 0

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



def assistant_loop(client: OpenAI, assistant_id, question=None, thread=None, notes=[], openapi_spec=None, environment='staging'):
    step_ids = set()
    if thread is None:
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
            output = None  # Initialize output variable
            
            if tool.function.name.startswith('ElasticVectorSearch'):
                # Extract bot and context names from tool name
                # e.g., ElasticVectorSearch_takanon_common_knowledge -> takanon, common_knowledge
                tool_name = tool.function.name[len('ElasticVectorSearch_'):]
                
                # Split by underscore and get bot name and context
                parts = tool_name.split('_', 1)  # Split only on first underscore
                bot_name = parts[0]
                context_name = parts[1] if len(parts) > 1 else ''
                
                # Load config to get context settings
                config_path = Path('specs') / bot_name / 'config.yaml'
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                    
                # Find matching context config by slug
                context_config = next(
                    (ctx for ctx in config.get('context', []) 
                     if ctx.get('slug') == context_name),
                    {}
                )
                
                # Use context-specific settings if available
                num_results = arguments.get('num_results', 
                                         context_config.get('max_num_results', 3))
                
                query_client = QueryClient(environment, bot_name, context_name)
                results = query_client.search(arguments['query'], num_results=num_results)
                
                # Format results for the assistant
                formatted_results = []
                for result in results:
                    formatted_results.append(
                        f"[Score: {result.score:.2f}]\n"
                        f"Content:\n{result.full_content}\n"
                        f"{'-' * 40}"
                    )
                output = "\n\n".join(formatted_results)
            
            elif tool.function.name == 'DatasetDBQuery':
                arguments['page_size'] = 30
                output = get_openapi_output(openapi_spec, tool.function.name, arguments)
            elif tool.function.name == 'DatasetInfo':
                output = get_dataset_info_cache(arguments, output)
            
            if output is not None:
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
    return thread