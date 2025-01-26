from ..collect_sources import collect_context_sources


class VectorStoreBase():

    def __init__(self, config_dir, production):
        self.config_dir = config_dir
        self.production = production
        self.tool_resources = None
        self.tools = []

    def vector_store_update(self, context, replace_context):
        for context_ in context:
            context_name = context_['name']
            if not self.production:
                context_name += ' - פיתוח'
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


class VectorStoreOpenAI(VectorStoreBase):

    def __init__(self, config_dir, production, openai_client):
        super().__init__(config_dir, production)
        self.openai_client = openai_client
        self.init = False

    def get_or_create_vector_store(self, context, context_name, replace_context):
        ret = None
        if not self.init:
            vector_store = self.openai_client.beta.vector_stores.list()
            for vs in vector_store:
                if vs.name == context_name:
                    if replace_context:
                        self.openai_client.beta.vector_stores.delete(vs.id)
                    else:
                        self.init = True
                        ret = vs
                    break
            if not self.init:
                vector_store = self.openai_client.beta.vector_stores.create(name=context_name)
                self.init = True
                ret = vector_store
        return ret

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        while len(file_streams) > 0:
            file_batch = self.openai_client.beta.vector_stores.file_batches.upload_and_poll(
                vector_store_id=vector_store.id, files=file_streams[:32]
            )
            if callable(callback):
                callback(file_batch.file_counts.completed)
            file_streams = file_streams[32:]

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
