from abc import ABC, abstractmethod
from ..collect_sources import collect_context_sources
from ..config import get_logger

logger = get_logger(__name__)

class VectorStoreBase(ABC):

    def __init__(self, config, config_dir, production):
        self.config = config
        self.config_dir = config_dir
        self.production = production
        self.tool_resources = None
        self.tools = []

    def env_name(self, name):
        if not self.production:
            name += ' - פיתוח'
        return name

    def env_name_slug(self, name):
        if not self.production:
            name += '__dev'
        return name

    def vector_store_update(self, context, replace_context, with_metadata=False):
        logger.info(f"Starting vector store update with metadata extraction: {with_metadata}")
        for context_ in context:
            context_name = context_['slug']
            vector_store = self.get_or_create_vector_store(context_, context_name, replace_context)
            
            # Collect sources with metadata if requested
            logger.info(f"Collecting context sources for {context_name} with metadata: {with_metadata}")
            file_streams = collect_context_sources(context_, self.config_dir, extract_metadata=with_metadata)
            logger.info(f"Collected {len(file_streams)} sources for {context_name}")
            
            # Adjust filenames for production/staging
            file_streams = [((fname if self.production else '_' + fname), f, t, m) for fname, f, t, m in file_streams]
            file_names = [fname for fname, _, _, _ in file_streams]
            
            deleted = self.delete_existing_files(context_, vector_store, file_names)
            logger.info(f"Deleted {deleted} existing files from {context_name}")
            print(f'VECTOR STORE {context_name} deleted {deleted}')
            
            total = len(file_streams)
            logger.info(f"Uploading {total} files to {context_name}")
            self.upload_files(context_, context_name, vector_store, file_streams, lambda x: print(f'VECTOR STORE {context_name} uploaded {x}/{total}'))
            
            self.update_tool_resources(context_, vector_store)
            self.update_tools(context_, vector_store)
        return self.tools, self.tool_resources

    @abstractmethod
    def get_or_create_vector_store(self, context, context_name, replace_context):
        pass

    @abstractmethod
    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        pass

    @abstractmethod
    def delete_existing_files(self, context_, vector_store, file_names):
        pass

    @abstractmethod
    def update_tools(self, context_, vector_store):
        pass

    @abstractmethod
    def update_tool_resources(self, context_, vector_store):
        pass
