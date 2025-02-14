import openai
import time
from dotenv import load_dotenv
from openai import OpenAI
import os
import argparse

load_dotenv()

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

client = OpenAI(
    api_key=OPENAI_API_KEY
)

def get_assistants():
    """
    Get all available assistants details
    """
    my_assistants_data = client.beta.assistants.list(
        order="desc",
        limit="20",
    )
    my_assistants = [{"id":assistant.id, "name": assistant.name} for assistant in my_assistants_data.data]
    return my_assistants

def start_conversation(assistant_id):
    """
    Creates a new conversation thread for the given assistant and
    continuously processes user inputs until '/stop' is typed.
    """
    # Create a new thread (a new conversation session)
    thread = openai.beta.threads.create()
    print(f"Created new thread with ID: {thread.id}")

    while True:
        # Read user input from the console
        user_input = input("User: ").strip()
        if user_input == "/stop":
            print("Conversation ended.")
            break

        # Add the user message to the thread
        openai.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=user_input
        )
        print("User message sent.")

        # Start a new run on the thread using the specified assistant
        run = openai.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant_id
        )
        print("Run started...")

        # Poll the run status until it completes or fails, with a timeout of 5 minutes
        start_time = time.time()
        timeout = 5 * 60  # 5 minutes in seconds
        while True:
            current_run = openai.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )
            print(f"Current run status: {current_run.status}")  # Debug print
            if current_run.status == "completed":
                break
            elif current_run.status == "failed":
                print(f"Run failed with error: {current_run.last_error}")
                break
            elif time.time() - start_time > timeout:
                print("Run polling timed out.")
                break
            time.sleep(2)

        # Once the run is complete, retrieve only the latest message
        messages_response = client.beta.threads.messages.list(
            thread_id=thread.id,
            order="desc",
            limit=1
        )

        if messages_response.data:
            latest_message = messages_response.data[0]
            if latest_message.role == "assistant":
                print(f"Assistant: {latest_message.content[0].text.value}")
            else:
                print("No assistant reply found.")
        else:
            print("No messages found.")

# based on the cli input (assistant_id), start the conversation with the assistant, if no assistant_id is provided, print the list of assistants and ask the user to select one
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start a conversation with an OpenAI assistant')
    parser.add_argument('--assistant-id', type=str, help='ID of the assistant to chat with')
    args = parser.parse_args()

    if args.assistant_id:
        start_conversation(args.assistant_id)
    else:
        assistants = get_assistants()
        
        # Print available assistants in a formatted way
        print("\nAvailable Assistants:")
        for idx, assistant in enumerate(assistants, 1):
            print(f"{idx}. {assistant['name']} (ID: {assistant['id']})")
        
        # Get assistant selection from user
        while True:
            try:
                selection = input("\nSelect an assistant number (or press Enter to input ID directly): ").strip()
                
                if selection == "":
                    # Direct ID input
                    assistant_id = input("Enter the assistant ID: ").strip()
                    if assistant_id:
                        break
                else:
                    # Selection by number
                    idx = int(selection) - 1
                    if 0 <= idx < len(assistants):
                        assistant_id = assistants[idx]['id']
                        print(f"\nSelected: {assistants[idx]['name']}")
                        break
                    else:
                        print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number or press Enter for direct ID input.")
        
        start_conversation(assistant_id)        