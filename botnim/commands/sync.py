import asyncio
import logging

async def sync_command(args):
    # ... existing synchronization logic ...
    
    # When uploading files:
    files_to_upload = prepare_files()  # Your existing file preparation
    uploaded_files = await upload_files(files_to_upload)
    
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        print("Uploaded Files:")
        for file in uploaded_files:
            print(f"- {file}")

def sync_command_wrapper(args):
    """Wrapper to run async sync_command"""
    asyncio.run(sync_command(args)) 