import openai
import time
from dotenv import load_dotenv
from openai import OpenAI
import os
import argparse
from typing import TypedDict

from .config import DEFAULT_ENVIRONMENT, get_openai_client

from .benchmark.assistant_loop import assistant_loop

load_dotenv()

class ToolOutput(TypedDict, total=False):
    output: str
    """The output of the tool call to be submitted to continue the run."""

    tool_call_id: str
    """
    The ID of the tool call in the `required_action` object within the run object
    the output is being submitted for.
    """

def get_assistants(client):
    """
    Get all available assistants details
    """
    my_assistants_data = client.beta.assistants.list(
        order="desc",
        limit="20",
    )
    my_assistants = [{"id":assistant.id, "name": assistant.name} for assistant in my_assistants_data.data]
    return my_assistants

def get_assistant_name(client, assistant_id):
    """
    Get the name of the assistant with the given ID
    """
    return client.beta.assistants.retrieve(assistant_id).name

def start_conversation(client, assistant_id, openapi_spec = None, rtl=False, environment=DEFAULT_ENVIRONMENT):
    """
    Creates a new conversation thread for the given assistant and
    continuously processes user inputs until '/stop' is typed.
    
    Args:
        assistant_id (str): The ID of the assistant to chat with
        rtl (bool): If True, reverses the output format for RTL languages
    """

    # get the assistant name
    assistant_name = get_assistant_name(client, assistant_id)

    if openapi_spec is not None and not openapi_spec.endswith(".yaml"):
        openapi_spec += ".yaml"

    # Create a new thread (a new conversation session)
    thread = client.beta.threads.create()
    prefix = f"{assistant_name}: "[::-1] if not rtl else f"{assistant_name}: "
    thread_msg = f"Created new thread with ID: {thread.id} (type /stop to end the conversation)"
    print(thread_msg[::-1] if rtl else thread_msg)

    while True:
        # Read user input from the console
        user_prompt = "משתמש-ת: "[::-1] if not rtl else "משתמש-ת: "
        user_input = input("\n---\n"+user_prompt[::-1]+"\n" if rtl else user_prompt).strip()
        if user_input == "/stop":
            end_msg = "השיחה הסתיימה."
            print(end_msg[::-1] if rtl else end_msg)
            break

        # Add the user message to the thread
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=user_input
        )
        
        sent_msg = "User message sent." if not rtl else "User message sent."[::-1]
        print(sent_msg[::-1] if rtl else sent_msg)

        assistant_loop(client, assistant_id, thread=thread, openapi_spec=openapi_spec, environment=environment)

        # Once the run is complete, retrieve only the latest message
        messages_response = client.beta.threads.messages.list(
            thread_id=thread.id,
            order="desc",
            limit=1
        )

        if messages_response.data:
            latest_message = messages_response.data[0]
            if latest_message.role == "assistant":
                message = latest_message.content[0].text.value
                if rtl:
                    # Reverse both the message and the prefix for RTL
                    print(f"{message[::-1]} {prefix[::-1]}")
                else:
                    print(f"{prefix}{message}")
            else:
                no_reply = "No assistant reply found."
                print(no_reply[::-1] if rtl else no_reply)
        else:
            no_msg = "No messages found."
            print(no_msg[::-1] if rtl else no_msg)

def assistant_main(assistant_id=None, openapi_spec=None, rtl=False, environment=DEFAULT_ENVIRONMENT):
    """
    Main function to start the assistant conversation
    
    Args:
        assistant_id (str, optional): ID of the assistant to chat with
        rtl (bool): Enable RTL support for Hebrew/Arabic
    """
    client = get_openai_client(environment)
    if assistant_id:
        start_conversation(client, assistant_id, openapi_spec=openapi_spec, rtl=rtl, environment=environment)
    else:
        assistants = get_assistants(client)
        
        # Print available assistants in a formatted way
        header = "\nAvailable Assistants:"
        print(header)
        for idx, assistant in enumerate(assistants, 1):
            assistant_id_text = f"ID: {assistant['id']}" if not rtl else f"ID: {assistant['id']}"[::-1]
            assistant_name_text = f"{assistant['name']}" if rtl else f"{assistant['name']}"[::-1]
            assistant_line = f"{idx}. {assistant_name_text}  - {assistant_id_text}"
            print(assistant_line[::-1] if rtl else assistant_line)
        
        # Get assistant selection from user
        while True:
            try:
                select_prompt = "\nSelect an assistant number (or press Enter to input ID directly): " if not rtl else "\nSelect an assistant number (or press Enter to input ID directly): "[::-1]
                selection = input(select_prompt[::-1] if rtl else select_prompt).strip()
                
                if selection == "":
                    # Direct ID input
                    id_prompt = "Enter the assistant ID: "
                    assistant_id = input(id_prompt[::-1] if rtl else id_prompt).strip()
                    if assistant_id:
                        break
                else:
                    # Selection by number
                    idx = int(selection) - 1
                    if 0 <= idx < len(assistants):
                        assistant_id = assistants[idx]['id']
                        selected_msg = f"\nSelected: {assistants[idx]['name']}"
                        print(selected_msg[::-1] if rtl else selected_msg)
                        break
                    else:
                        error_msg = "Invalid selection. Please try again."
                        print(error_msg[::-1] if rtl else error_msg)
            except ValueError:
                error_msg = "Please enter a valid number or press Enter for direct ID input."
                print(error_msg[::-1] if rtl else error_msg)
        
        start_conversation(client, assistant_id, openapi_spec=openapi_spec, rtl=rtl, environment=environment)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start a conversation with an OpenAI assistant')
    parser.add_argument('--assistant-id', type=str, help='ID of the assistant to chat with')
    parser.add_argument('--openapi-spec', type=str, default='budgetkey', help='either "budgetkey" or "takanon"')
    parser.add_argument('--rtl', action='store_true', help='Enable RTL support for Hebrew/Arabic')
    parser.add_argument('--environment', type=str, default=DEFAULT_ENVIRONMENT, help='Specify the environment')
    args = parser.parse_args()
    assistant_main(args.assistant_id, args.openapi_spec + ".yaml", args.rtl, environment=args.environment)