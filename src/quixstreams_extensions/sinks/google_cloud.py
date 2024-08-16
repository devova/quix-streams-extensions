import time
from pathlib import Path
from typing import Optional, Callable, Any, Union, List, Tuple, Dict
from operator import attrgetter

from google.cloud import firestore
from google.cloud.firestore_v1 import CollectionReference, DocumentReference, WriteBatch
from quixstreams.models import SerializationContext
from quixstreams.sinks import BatchingSink, SinkBatch
from quixstreams.sinks.base.item import SinkItem
from rocksdict import Rdict, WriteBatch as RocksDictWriteBatch


class GoogleFirestoreFlatSink(BatchingSink):
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


class GoogleFirestoreNestedSink(BatchingSink):
    """
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

        And you would like to keep a logical structure of your data, like a tree structure:

        stores
        "123"  -> daily
                  "2024-07-13"  ->  balance-per-user-id
                                    "3640832"           -> {"balance": 100}

        All you need is to define **collections_structure**,
        a path to the deepest node as a list of collection items via its name and key:

        GoogleFirestoreNestedSink(
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
        collections_structure: List[Tuple[str, Callable[[SinkItem], str]]],
        client: Optional[firestore.Client] = None,
        value: Optional[Callable[[SinkItem], dict]] = None,
        value_serializer: Optional[Callable[[Any, SerializationContext], dict]] = None,
        state_dir: str = "state",
    ):
        super().__init__()
        self._collections_structure = collections_structure
        self._db = client or firestore.Client()
        self._value = value or attrgetter("value")
        self._value_serializer = value_serializer

        # rocks db keep track of what nodes has been already created, to reduce amount of Firestore writes
        self._cache_db = self._init_rocksdb(str(Path(state_dir).absolute()))
        self._cache: Dict[str, Rdict] = {}

    def __del__(self):
        if self._cache_db is not None:
            self._cache_db.close()

    @classmethod
    def _init_rocksdb(cls, state_dir) -> Rdict:
        attempt = 1
        open_max_retries = 10
        while True:
            try:
                db = Rdict(str((Path(state_dir) / cls.__name__).absolute()))
                return db
            except Exception as exc:
                is_locked = str(exc).lower().startswith("io error")
                if not is_locked:
                    raise
                if open_max_retries <= 0 or attempt >= open_max_retries:
                    raise
                attempt += 1
                time.sleep(3)

    def _init_column_family(self, topic: str) -> Rdict:
        if topic not in self._cache:
            try:
                return self._cache_db.get_column_family(topic)
            except Exception:
                return self._cache_db.create_column_family(topic)

    def _get_last_document(
        self,
        batch: WriteBatch,
        item: SinkItem,
        rocks_db: Rdict,
        in_mem_cache: dict,
        rocks_db_batch: RocksDictWriteBatch,
    ) -> DocumentReference:
        cache_key = ""
        last_document_ref = self._db
        for collection_name, document_key_cb in self._collections_structure:
            document_key = document_key_cb(item)
            cache_key += f"/{collection_name}/{document_key}"
            last_document_ref = self._db.document(cache_key[1:])
            if cache_key not in in_mem_cache and cache_key not in rocks_db:
                # no worries if it exists in Firestore, we need to populate the cache
                batch.set(last_document_ref, {".tap": True})
                in_mem_cache[cache_key] = True
                rocks_db_batch[cache_key] = True
        return last_document_ref

    def write(self, batch: SinkBatch):
        db_batch = self._db.batch()
        if batch.topic not in self._cache:
            self._cache[batch.topic] = self._init_column_family(batch.topic)
        in_mem_cache = {}
        wb = RocksDictWriteBatch()
        for item in batch:
            if self._value_serializer:
                ctx = SerializationContext(batch.topic, headers=item.headers)
                value = self._value_serializer(self._value(item), ctx)
            else:
                value = self._value(item)
            document_ref = self._get_last_document(db_batch, item, self._cache[batch.topic], in_mem_cache, wb)
            db_batch.set(document_ref, value)
        db_batch.commit()
        self._cache_db.write(wb)
