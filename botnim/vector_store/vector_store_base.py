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

    def vector_store_update(self, context, replace_context, reindex=False, force_rebuild=False):
        self.tool_resources = None
        self.tools = []
        for context_ in context:
            context_name = context_['slug']
            # `replace_context` selects WHICH contexts to process this run:
            #   'all'       -> every context (delta semantics by default)
            #   '<slug>'    -> just that context
            #   'none'      -> explicit no-op for the data layer
            #   None        -> treated as 'all' for back-compat (callers
            #                  using positional / no-flag CLI)
            # `force_rebuild` (when True AND this context is being processed)
            # adds a DELETE-then-re-embed; without it, upload_files's
            # content-hash skip handles the delta naturally.
            normalized = replace_context if replace_context is not None else 'all'
            if normalized == 'none':
                # `reindex` is the explicit "force processing regardless of
                # selection" override and beats even an explicit 'none'.
                should_process = bool(reindex)
            elif normalized == 'all' or normalized == context_name:
                should_process = True
            else:
                should_process = bool(reindex)
            should_force_rebuild = force_rebuild and should_process

            vector_store = self.get_or_create_vector_store(
                context_, context_name, should_process, force_rebuild=should_force_rebuild,
            )

            if should_process:
                if force_rebuild or reindex:
                    print(f'Processing context (force_rebuild={should_force_rebuild}, reindex={reindex}): {context_name}')
                else:
                    print(f'Processing context (delta): {context_name}')
                file_streams = collect_context_sources(context_, self.config_dir)
                file_streams = [((fname if self.production else '_' + fname), f, t, m) for fname, f, t, m in file_streams]
                file_names = [fname for fname, _, _, _ in file_streams]

                # Force-rebuild path: existing files were already wiped via
                # get_or_create_vector_store. Skip the per-name delete.
                if not should_force_rebuild:
                    deleted = self.delete_existing_files(context_, vector_store, file_names)
                    print(f'VECTOR STORE {context_name} deleted {deleted}')

                total = len(file_streams)
                self.upload_files(context_, context_name, vector_store, file_streams,
                                  lambda x: print(f'VECTOR STORE {context_name} uploaded {x}/{total}'))

            self.update_tool_resources(context_, vector_store)
            self.update_tools(context_, vector_store)
        return self.tools, self.tool_resources

    @abstractmethod
    def get_or_create_vector_store(self, context, context_name, replace_context, force_rebuild=False):
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