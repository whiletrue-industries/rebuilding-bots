from .vector_store_base import VectorStoreBase
import json
import io
from ..config import get_logger

logger = get_logger(__name__)


class VectorStoreOpenAI(VectorStoreBase):

    def __init__(self, config, config_dir, production, openai_client):
        super().__init__(config, config_dir, production)
        self.openai_client = openai_client
        self.init = False

    def get_or_create_vector_store(self, context, context_name, replace_context):
        ret = None
        vs_name = self.env_name(self.config['name'])
        vector_store = self.openai_client.beta.vector_stores.list()
        for vs in vector_store:
            if vs.name == vs_name:
                if replace_context and not self.init:
                    self.openai_client.beta.vector_stores.delete(vs.id)
                else:
                    ret = vs
                break
        if not ret:
            assert not self.init, 'Attempt to create a new vector store after initialization'
            vector_store = self.openai_client.beta.vector_stores.create(name=vs_name)
            ret = vector_store
        self.init = True
        return ret

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        count = 0
        logger.info(f"Starting upload of {len(file_streams)} files to OpenAI vector store")
        
        # Process file streams to include metadata in content
        processed_streams = []
        for filename, content_file, file_type, metadata in file_streams:
            try:
                # Read the original content
                content = content_file.read()
                
                # If metadata exists, append it to the content
                if metadata:
                    # Convert metadata to a formatted string
                    metadata_str = json.dumps(metadata, indent=2, ensure_ascii=False)
                    
                    # Create a new content stream with metadata appended
                    combined_content = content + f"\n\n-------\n# Metadata:\n\n{metadata_str}".encode('utf-8')
                    
                    # Create a new BytesIO object with the combined content
                    new_content_file = io.BytesIO(combined_content)
                    
                    logger.info(f"Appended metadata to content for {filename}")
                else:
                    # If no metadata, just rewind the stream and use as is
                    new_content_file = io.BytesIO(content)
                
                # Add the file to the processed streams
                processed_streams.append((filename, new_content_file, file_type))
                
            except Exception as e:
                logger.error(f"Failed to process file {filename}: {str(e)}")
        
        logger.info(f"Processed {len(processed_streams)} files for upload")
        
        # Continue with the original upload logic
        file_streams = processed_streams
        while len(file_streams) > 0:
            current = file_streams[:32]
            try:
                logger.info(f"Uploading batch of {len(current)} files")
                file_batch = self.openai_client.beta.vector_stores.file_batches.upload_and_poll(
                    vector_store_id=vector_store.id, files=current
                )
                logger.info(f"Batch upload completed: {file_batch.id}")
            except Exception as e:
                logger.error(f"Failed to upload batch: {str(e)}")
            
            count += len(current)
            if callable(callback):
                callback(count)
            file_streams = file_streams[32:]
        
        logger.info(f"Completed upload of {count} files to OpenAI vector store")

    def delete_existing_files(self, context_, vector_store, file_names):
        deleted = 0
        for ef in self.openai_client.files.list():
            if ef.filename in file_names:
                self.openai_client.files.delete(ef.id)
                deleted += 1
        return deleted

    def update_tools(self, context_, vector_store):
        if len(self.tools) == 0:
            self.tools.append(dict(
                type='file_search',
                file_search=dict(
                    max_num_results=context_.get('max_num_results', 20),
                ),
            ))

    def update_tool_resources(self, context_, vector_store):
        if self.tool_resources is None:
            self.tool_resources = dict(
                file_search=dict(
                    vector_store_ids=[vector_store.id],
                ),
            )
