from .collect_sources import collect_context_sources

def vector_store_update(context, config_dir, production, replace_context, openai_client):
    for context_ in context:
        context_name = context_['name']
        if not production:
            context_name += ' - פיתוח'
        vector_store = openai_client.beta.vector_stores.list()
        vector_store_id = None
        for vs in vector_store:
            if vs.name == context_name:
                if replace_context:
                    openai_client.beta.vector_stores.delete(vs.id)
                else:
                    vector_store_id = vs.id
                break
        if vector_store_id is None:
            file_streams = collect_context_sources(context_, config_dir)
            file_streams = [((fname if production else '_' + fname), f, t) for fname, f, t in file_streams]
            existing_files = openai_client.files.list()
            # delete existing files:
            deleted = 0
            for fname, _, _ in file_streams:
                for ef in existing_files:
                    if ef.filename == fname:
                        openai_client.files.delete(ef.id)
                        deleted += 1
            vector_store = openai_client.beta.vector_stores.create(name=context_name)
            while len(file_streams) > 0:
                file_batch = openai_client.beta.vector_stores.file_batches.upload_and_poll(
                    vector_store_id=vector_store.id, files=file_streams[:32]
                )
                print(f'VECTOR STORE {context_name} batch: ' + \
                        f'deleted {deleted}, ' + \
                        f'uploaded {file_batch.file_counts.completed}, ' + \
                        f'failed {file_batch.file_counts.failed}, ' + \
                        f'pending {file_batch.file_counts.in_progress}, ' + \
                        f'remaining {len(file_streams)}')
                file_streams = file_streams[32:]
            vector_store_id = vector_store.id
        tool_resources = dict(
            file_search=dict(
                vector_store_ids=[vector_store_id],
            ),
        )
    tools = [dict(
        type='file_search',
        file_search=dict(
            max_num_results=context_.get('max_num_results', 20),
        ),
    )]
    return tools, tool_resources
