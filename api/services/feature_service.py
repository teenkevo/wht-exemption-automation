"""Online features for one taxpayer, read from the Feast online store.

``make features`` registers the view and ``make materialize`` fills the store.
The grant lookup does not use this service.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from data_engineering.config import ProjectConfig
from features.registry import VIEW_NAME, check_online_store, open_store


class FeaturesNotReady(RuntimeError):
    """The feature registry or the online store is not available."""


class TaxpayerNotInStore(LookupError):
    """The TIN has no features in the online store."""


@dataclass
class FeatureService:
    config: ProjectConfig
    _store: object | None = field(default=None, init=False, repr=False)
    _names: list[str] = field(default_factory=list, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    @classmethod
    def load(cls) -> FeatureService:
        return cls(config=ProjectConfig.load())

    def _open(self):
        with self._lock:
            if self._store is not None:
                return self._store
            if not self.config.feast_registry.exists():
                raise FeaturesNotReady("Feature registry not found. Run make features")
            try:
                check_online_store(self.config)
            except ConnectionError as exc:
                raise FeaturesNotReady(str(exc)) from exc
            store = open_store(self.config)
            view = store.get_feature_view(VIEW_NAME)
            self._names = [item.name for item in view.features]
            self._store = store
            return store

    def get_online(self, entity_id: str) -> dict:
        import redis

        store = self._open()
        try:
            values = store.get_online_features(
                features=[f"{VIEW_NAME}:{name}" for name in self._names],
                entity_rows=[{"TIN_NO": entity_id}],
            ).to_dict()
        except redis.exceptions.ConnectionError as exc:
            raise FeaturesNotReady("Online store is not reachable") from exc
        row = {name: values[name][0] for name in self._names}
        if all(value is None for value in row.values()):
            raise TaxpayerNotInStore("Taxpayer has no features in the online store")
        return row
