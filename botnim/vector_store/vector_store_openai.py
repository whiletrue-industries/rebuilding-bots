from .vector_store_base import VectorStoreBase


class VectorStoreOpenAI(VectorStoreBase):

    def __init__(self, config, config_dir, production, openai_client):
        super().__init__(config, config_dir, production)
        self.openai_client = openai_client
        self.init = False

    def get_or_create_vector_store(self, context, context_name, replace_context, force_rebuild=False):
        """Get or create the OpenAI Vector Store for this bot.

        OpenAI Vector Stores have no per-row content-hash skip (unlike Aurora),
        so any sync that processes a context is effectively a full rebuild.
        ``replace_context`` is preserved for caller-API compatibility and is
        OR'd with ``force_rebuild`` to gate the destructive delete.

        With the new sync default ``replace_context='all'`` (post-delta-sync),
        every sync wipes and re-uploads the OpenAI Vector Store. The previous
        no-op-by-default behavior now requires explicit ``--replace-context none``.
        This backend is legacy (see CLAUDE.md); Aurora is the canonical path.
        """
        ret = None
        vs_name = self.env_name(self.config['name'])
        vector_store = self.openai_client.beta.vector_stores.list()
        for vs in vector_store:
            if vs.name == vs_name:
                if (replace_context or force_rebuild) and not self.init:
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
        while len(file_streams) > 0:
            current = file_streams[:32]
            file_batch = self.openai_client.beta.vector_stores.file_batches.upload_and_poll(
                vector_store_id=vector_store.id, files=current
            )
            count += len(current)
            if callable(callback):
                callback(count)
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
