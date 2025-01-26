from .vector_store_base import VectorStoreBase


class VectorStoreOpenAI(VectorStoreBase):

    def __init__(self, config, config_dir, production, openai_client):
        super().__init__(config, config_dir, production)
        self.openai_client = openai_client
        self.init = False

    def get_or_create_vector_store(self, context, context_name, replace_context):
        ret = None
        if not self.init:
            vs_name = self.env_name(self.config['name'])
            vector_store = self.openai_client.beta.vector_stores.list()
            for vs in vector_store:
                if vs.name == vs_name:
                    if replace_context:
                        self.openai_client.beta.vector_stores.delete(vs.id)
                    else:
                        self.init = True
                        ret = vs
                    break
            if not self.init:
                vector_store = self.openai_client.beta.vector_stores.create(name=vs_name)
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
