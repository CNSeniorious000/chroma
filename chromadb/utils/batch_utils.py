from typing import Optional, Tuple, List
from chromadb.api import BaseAPI
from chromadb.api.types import (
    Documents,
    Embeddings,
    IDs,
    Metadatas,
)


def create_batches(
    api: BaseAPI,
    ids: IDs,
    embeddings: Optional[Embeddings] = None,
    metadatas: Optional[Metadatas] = None,
    documents: Optional[Documents] = None,
) -> List[Tuple[IDs, Embeddings, Optional[Metadatas], Optional[Documents]]]:
    _batches: List[
        Tuple[IDs, Embeddings, Optional[Metadatas], Optional[Documents]]
    ] = []
    if len(ids) > api.max_batch_size:
        # create split batches
        _batches.extend(
            (
                ids[i : i + api.max_batch_size],
                embeddings[i : i + api.max_batch_size] if embeddings else None,
                metadatas[i : i + api.max_batch_size] if metadatas else None,
                documents[i : i + api.max_batch_size] if documents else None,
            )
            for i in range(0, len(ids), api.max_batch_size)
        )
    else:
        _batches.append((ids, embeddings, metadatas, documents))  # type: ignore
    return _batches
