class RedisDB:
    """In-memory key-value store (Redis-style bulk strings as bytes)."""

    def __init__(self):
        self.store = {}

    def get(self, key: bytes) -> bytes | None:
        return self.store.get(key, None)

    def set(self, key: bytes, value: bytes) -> None:
        self.store[key] = value
