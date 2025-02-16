from abc import ABC, abstractmethod
from ..collect_sources import collect_context_sources


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

    def vector_store_update(self, context, replace_context):
        for context_ in context:
            context_name = context_['slug']
            vector_store = self.get_or_create_vector_store(context_, context_name, replace_context)
            file_streams = collect_context_sources(context_, self.config_dir)
            file_streams = [((fname if self.production else '_' + fname), f, t) for fname, f, t in file_streams]
            file_names = [fname for fname, _, _ in file_streams]
            deleted = self.delete_existing_files(context_, vector_store, file_names)
            print(f'VECTOR STORE {context_name} deleted {deleted}')
            total = len(file_streams)
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