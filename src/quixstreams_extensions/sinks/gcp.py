from typing import Optional, Callable, Any, Union, List, Tuple, Dict
from operator import attrgetter

from google.cloud import firestore
from google.cloud.firestore_v1 import CollectionReference, DocumentReference, WriteBatch
from quixstreams.models import SerializationContext
from quixstreams.sinks import BatchingSink, SinkBatch
from quixstreams.sinks.base.item import SinkItem


class FlatGoogleFirestoreSink(BatchingSink):
    """
    A simple key-value sink.
    It puts all data as a flat structure into a specified collection.
    Guarantees one Firestore write per message.
    Doesn't perform any Firestore reads.
    """

    def __init__(
        self,
        collection: Union[str, CollectionReference],
        client: Optional[firestore.Client] = None,
        key: Optional[Callable[[SinkItem], str]] = None,
        key_serializer: Optional[Callable[[Any, SerializationContext], str]] = None,
        value: Optional[Callable[[SinkItem], str]] = None,
        value_serializer: Optional[Callable[[Any, SerializationContext], dict]] = None,
    ):
        super().__init__()
        self._db = client or firestore.Client()
        self._collection = (
            collection if isinstance(collection, CollectionReference) else self._db.collection(collection)
        )
        self._key = key or attrgetter("key")
        self._key_serializer = key_serializer
        self._value = value or attrgetter("value")
        self._value_serializer = value_serializer

    def write(self, batch: SinkBatch):
        db_batch = self._db.batch()
        for item in batch:
            ctx = SerializationContext(batch.topic, headers=item.headers)
            key = self._key_serializer(self._key(item), ctx) if self._key_serializer else self._key(item)
            value = self._value_serializer(self._value(item), ctx) if self._value_serializer else self._value(item)

            db_batch.set(self._collection.document(key), value)
        db_batch.commit()


class _NestedGoogleFirestoreSink(BatchingSink):
    """
    Experimental. May lead to high memory usage.

    A comprehensive sink that allows putting data into a nested tree-like structure.
    May perform more Firestore writes than incoming messages due to serving nested structure creation.
    Doesn't perform any Firestore reads.

    Example:
        Imagine you have a SinkItem like:

        {
           "key": {"user_id": 3640832, 'store_number': 123},
           "value": {"balance": 100},
           "timestamp": 1720828800000,
           "headers": None
        }

        And you would like to keep a logical structure of your data, like:

        stores -> "123"
                  daily -> "2024-07-13"
                           balance-per-user -> "3640832" = {"balance": 100}

        All you need is to define **keys_structure**:

        _NestedGoogleFirestoreSink(
            [
                ("stores", get_store_key),

                ("daily", get_day_key),

                ("balance-per-user", get_user_key),
            ]
        )

        Where `get_store_key`, `get_day_key` and `get_user_key` are Callable[[SinkItem], str]
    """

    def __init__(
        self,
        keys_structure: List[Tuple[str, Callable[[SinkItem], str]]],
        client: Optional[firestore.Client] = None,
        value: Optional[Callable[[SinkItem], str]] = None,
        value_serializer: Optional[Callable[[Any, SerializationContext], dict]] = None,
    ):
        super().__init__()
        self._keys_structure = keys_structure
        self._db = client or firestore.Client()
        self._value = value or attrgetter("value")
        self._value_serializer = value_serializer
        self._existing_nodes_cache: Dict[str, DocumentReference] = {}  # TODO: memory overflow, rocksdb needs to be here

    def _get_last_document(self, batch: WriteBatch, item: SinkItem) -> DocumentReference:
        cache_key = ""
        last_document_ref = self._db
        for collection_name, document_key_cb in self._keys_structure:
            document_key = document_key_cb(item)
            cache_key += f"{collection_name}{document_key}"
            if cache_key not in self._existing_nodes_cache:
                # no worries if it exists in Firestore, we need to populate the cache
                self._existing_nodes_cache[cache_key] = last_document_ref.collection(collection_name).document(
                    document_key
                )
                batch.set(self._existing_nodes_cache[cache_key], {".tap": True})
            last_document_ref = self._existing_nodes_cache[cache_key]
        return last_document_ref

    def write(self, batch: SinkBatch):
        db_batch = self._db.batch()
        for item in batch:
            if self._value_serializer:
                ctx = SerializationContext(batch.topic, headers=item.headers)
                value = self._value_serializer(self._value(item), ctx)
            else:
                value = self._value(item)
            document_ref = self._get_last_document(db_batch, item)
            db_batch.set(document_ref, value)
        db_batch.commit()
