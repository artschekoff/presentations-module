from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Iterable

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection

from presentations_module.core.presentation_document import PresentationDocument


class MongoStorage:
    """Thin, fast wrapper around a MongoDB collection for presentation metadata."""

    def __init__(
        self,
        uri: str | None = None,
        database: str | None = None,
        collection: str | None = None,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._uri = uri or os.environ["MONGODB_URI"]
        self._db_name = database or os.environ["MONGODB_DB_NAME"]
        self._collection_name = collection or os.environ["MONGODB_COLLECTION"]

        kwargs = {
            # Low timeouts keep failures quick; retryWrites improves resiliency.
            "serverSelectionTimeoutMS": 2000,
            "connectTimeoutMS": 2000,
            "retryWrites": True,
        }
        if client_kwargs:
            kwargs.update(client_kwargs)

        self._client = MongoClient(self._uri, **kwargs)
        self._collection: Collection = self._client[self._db_name][
            self._collection_name
        ]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._collection.create_index(
            [("topic", ASCENDING), ("language", ASCENDING)], background=True
        )
        self._collection.create_index([("created_at", DESCENDING)], background=True)

    def save_presentation(
        self,
        *,
        document: PresentationDocument,
        extra: dict[str, Any] | None = None,
    ) -> ObjectId:
        payload = document.payload()

        if extra:
            payload.update(extra)

        result = self._collection.insert_one(payload)

        return result.inserted_id

    def save_error(
        self,
        id: ObjectId,
        error: str,
    ) -> ObjectId:
        payload: dict[str, Any] = {
            "status": "failed",
            "error": error,
            "completed_at": datetime.utcnow(),
        }

        result = self._collection.update_one({"_id": id}, {"$set": payload})

        return result.upserted_id

    def save_result(
        self,
        id: ObjectId,
        files: Iterable[str],
    ) -> ObjectId:
        payload: dict[str, Any] = {
            "status": "completed",
            "files": list(files),
            "completed_at": datetime.utcnow(),
        }

        result = self._collection.update_one({"_id": id}, {"$set": payload})

        return result.upserted_id

    def get_generation(self, record_id: str | ObjectId) -> dict[str, Any] | None:
        object_id = (
            record_id if isinstance(record_id, ObjectId) else ObjectId(str(record_id))
        )
        return self._collection.find_one({"_id": object_id})

    def list_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        cursor = (
            self._collection.find().sort("created_at", DESCENDING).limit(max(limit, 1))
        )
        return list(cursor)

    def close(self) -> None:
        self._client.close()


_cached_storage: MongoStorage | None = None


def get_storage() -> MongoStorage:
    """Return a cached MongoStorage instance to avoid extra connections."""
    global _cached_storage
    if _cached_storage is None:
        _cached_storage = MongoStorage()
    return _cached_storage
