import yaml
import json
import os
from pathlib import Path
import requests_openapi
from typing import Dict
import re
import logging

from openai import OpenAI
from openai.types.beta.threads.runs.run_step import ToolCallsStepDetails

from botnim.query import QueryClient, run_query
from botnim.config import get_logger, DEFAULT_ENVIRONMENT, SPECS
TEMP = 0

logger = get_logger(__name__)

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

def assistant_loop(client: OpenAI, assistant_id, question=None, thread=None, notes=[], openapi_spec=None, environment=DEFAULT_ENVIRONMENT):
    # Initialize or append to log file in the same directory as the script
    log_file = Path(__file__).parent / 'log.txt'
    with open(log_file, 'w', encoding='utf-8') as f:  # open for writing, truncate the file
        f.write(f"\n=== New Conversation ===\nAssistant ID: {assistant_id}\nEnvironment: {environment}\n\n")

    step_ids = set()
    if thread is None:
        thread = client.beta.threads.create()
    if question:
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role='user',
            content=question
        )
        # Log initial question
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"User Question: {question}\nThread ID: {thread.id}\n\n")

    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread.id,
        assistant_id=assistant_id,
        temperature=TEMP,
        top_p=1,
    )
    assert run.status in ['completed', 'requires_action']
    
    # Log new run
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"=== New Run ===\nRun ID: {run.id}\nStatus: {run.status}\n\n")

    while True:
        print('RUN', run.id, run.status, run.temperature, run.top_p)
        tool_outputs = []
        notes.append(f'RUN {run.status}')
        
        # Log run status
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"Run Status: {run.status}\n")

        for step in client.beta.threads.runs.steps.list(run.id, thread_id=thread.id, order='asc', extra_query=dict(include=['step_details.tool_calls[*].file_search.results[*].content'])):
            if step.id in step_ids:
                continue
            step_ids.add(step.id)
            if step.type == 'tool_calls':
                step_details: ToolCallsStepDetails = step.step_details
                for tool_call in step_details.tool_calls:
                    if tool_call.type == 'function':
                        # Log function calls
                        with open(log_file, 'a', encoding='utf-8') as f:
                            f.write(f"\nTool Call:\n  Type: function\n  Name: {tool_call.function.name}\n  Arguments: {tool_call.function.arguments}\n")
                        # Restore notes functionality
                        print('TOOL', tool_call.id, tool_call.function.name, tool_call.function.arguments)
                        notes.append(f'{tool_call.function.name}({tool_call.function.arguments})')
                    elif tool_call.type == 'file_search':
                        # Log file searches
                        with open(log_file, 'a', encoding='utf-8') as f:
                            f.write(f"\nTool Call:\n  Type: file_search\n  Query: {tool_call.file_search}\n")
                            for result in (tool_call.file_search.results or []):
                                text = result.content[0].text if result.content else None
                                if text:
                                    f.write(f"  Result:\n{text}\n")
                        # Restore notes functionality
                        print('FILE-SEARCH', tool_call.id, tool_call.file_search)
                        notes.append(f'file-search:')
                        for result in (tool_call.file_search.results or []):
                            text = result.content[0].text if result.content else None
                            if text:
                                notes.append(f'>>\n{text}\n<<')

        if run.status == 'completed':
            # Log assistant's response when run is completed
            messages = client.beta.threads.messages.list(thread_id=thread.id, order='desc', limit=1)
            for message in messages:
                if message.role == "assistant":
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write("\nAssistant Response:\n")
                        for content in message.content:
                            if content.type == 'text':
                                f.write(f"{content.text.value}\n")
                                break  # Only write the latest response
            
            # Log completion
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write("\n=== Run Completed ===\n\n")
            break

        for tool in run.required_action.submit_tool_outputs.tool_calls:
            arguments = json.loads(tool.function.arguments)
            output = None  # Initialize output variable
            
            # Log tool input
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"\nTool Input:\n  Tool ID: {tool.id}\n  Name: {tool.function.name}\n  Arguments:\n")
                for key, value in arguments.items():
                    f.write(f"    {key}: {value}\n")
            
            # Handle different tool types
            if tool.function.name.startswith('search_'):
                # Remove 'search_' prefix and '__dev' suffix if present
                tool_name = tool.function.name[len('search_'):]
                if tool_name.endswith('__dev'):
                    tool_name = tool_name[:-len('__dev')]
                
                # Split into bot_name and context_name
                parts = tool_name.split('__', 1)
                bot_name = parts[0]
                context_name = parts[1] if len(parts) > 1 else ''
                
                # Get num_results from arguments if provided, otherwise pass None to use context default
                num_results = arguments.get('num_results')
                
                # Log the tool call parameters
                logger.info(f"Calling run_query with query: {arguments['query']}, num_results: {num_results}")
                
                output = run_query(
                    environment=environment,
                    bot_name=bot_name,
                    context_name=context_name,
                    query=arguments['query'],
                    num_results=num_results,
                    format="text"
                )
                
                # Format results for better readability if output is a dictionary with results
                if isinstance(output, dict) and 'results' in output:
                    formatted_results = []
                    for i, result in enumerate(output.get('results', [])):
                        formatted_results.append(f"**Result {i+1}:**\n{result.get('content', '')}\n")
                    
                    # Store the original results but provide a formatted version for display
                    response = {
                        'results': '\n'.join(formatted_results),
                        'raw_results': output.get('results', [])
                    }
                    formatted_output = json.dumps(response)
                    
                    # Add output file generation for traceability
                    output_file = f'output/{bot_name}_{context_name}.json'
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    with open(output_file, 'w') as f:
                        json.dump(output, f, indent=2)
                    
                    logger.info(f"Saved search results to {output_file}")
                    
                    # For logging purposes
                    logger.info(f"Tool output (formatted): {formatted_output[:200]}...")
                else:
                    # Log the output as is
                    logger.info(f"Tool output: {output}")
            
            # Handle all non-search tools with OpenAPI
            else:
                # Set default page_size for DatasetDBQuery
                if tool.function.name == 'DatasetDBQuery':
                    arguments['page_size'] = 30
                
                # Call get_openapi_output for all non-search tools
                try:
                    if openapi_spec is not None:
                        output = get_openapi_output(openapi_spec, tool.function.name, arguments)
                        
                        if tool.function.name == 'DatasetInfo':
                            # Special case for DatasetInfo
                            output = get_dataset_info_cache(arguments, output)
                    else:
                        output = f"Error: No OpenAPI spec provided for bot. Cannot execute tool '{tool.function.name}'"
                except Exception as e:
                    logger.error(f"Error calling {tool.function.name}: {e}")
                    output = f"Error: {str(e)}"
            
            if output is not None:
                # Log tool output
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"\nTool Output:\n  Tool ID: {tool.id}\n  Content:\n")
                    if isinstance(output, str):
                        f.write(f"    {output}\n")
                    else:
                        # For dictionary/list outputs, format them nicely
                        # Check if this is a search result with metadata
                        if isinstance(output, list) and output and 'metadata' in output[0]:
                            f.write("    Results with metadata:\n")
                            for item in output:
                                f.write(f"    --- Result ID: {item.get('id', 'unknown')} ---\n")
                                f.write(f"    Content: {item.get('content', '')[:100]}...\n")
                                f.write(f"    Score: {item.get('score', 'N/A')}\n")
                                f.write(f"    Metadata: {json.dumps(item.get('metadata', {}), ensure_ascii=False, indent=6)}\n\n")
                        else:
                            # Standard formatting for other outputs
                            output_str = json.dumps(output, ensure_ascii=False, indent=4)
                            # Add indentation to each line
                            formatted_output = "\n".join(f"    {line}" for line in output_str.split("\n"))
                            f.write(f"{formatted_output}\n")
                
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
