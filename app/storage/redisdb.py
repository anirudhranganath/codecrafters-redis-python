import time


class RedisDB:
    """In-memory key-value store (Redis-style bulk strings as bytes)."""

    def __init__(self):
        self.store = {}

    def get(self, key: bytes) -> bytes | None:
        """Get the value associated with the key. Returns None if the key is not found or expired."""
        if key not in self.store:
            return None

        value, expiry_at = self.store[key]
        if expiry_at is not None and time.time() * 1000 >= expiry_at:
            del self.store[key]
            return None

        return value

    def set(self, key: bytes, value: bytes, px: int | None = None) -> None:
        """Set the value associated with the key, with optional expiry in milliseconds."""
        expiry_at = None
        if px is not None:
            expiry_at = (time.time() * 1000) + px

        self.store[key] = (value, expiry_at)

    def rpush(self, key: bytes, value: list[bytes]) -> int:
        """
        Append value to the list at a key. Creates a new list if key doesn't exist.
        Returns the length of the list after the push.
        """
        if key not in self.store:
            self.store[key] = []
        if type(self.store[key]) is not list:
            raise TypeError("Value at key is not a list")
        self.store[key].extend(value)
        return len(self.store[key])

    def lrange(self, key: bytes, start_index: bytes, end_index: bytes) -> [bytes]:
        """
        Return values between indices start_index and end_index (inclusive)
        of the list at a key.
        """
        if key not in self.store or start_index > end_index:
            return []
        if type(self.store[key]) is not list:
            raise TypeError("Value at key is not a list")
        if start_index >= len(self.store[key]):
            return []
        return self.store[key][start_index:end_index + 1]