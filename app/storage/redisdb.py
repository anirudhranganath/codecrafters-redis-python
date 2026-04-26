import time

from app.storage.strobj import RedisEntry, RedisType


class WrongTypeError(Exception):
    """Raised when an operation is performed on a key holding the wrong type."""


class RedisDB:
    """In-memory key-value store (Redis-style bulk strings as bytes)."""

    def __init__(self):
        self.store: dict[bytes, RedisEntry] = {}

    def _get_entry(self, key: bytes) -> RedisEntry | None:
        """Return the entry for key, deleting it if expired. Returns None if missing or expired."""
        entry = self.store.get(key)
        if entry is None:
            return None
        if entry.expiry_ms is not None and time.time_ns() // 1_000_000 >= entry.expiry_ms:
            del self.store[key]
            return None
        return entry

    def get(self, key: bytes) -> bytes | None:
        """Get the string value for key. Returns None if missing or expired. Raises WrongTypeError if key holds a list."""
        entry = self._get_entry(key)
        if entry is None:
            return None
        if entry.type != RedisType.STRING:
            raise WrongTypeError
        return entry.value  # type: ignore[return-value]

    def set(self, key: bytes, value: bytes, px: int | None = None) -> None:
        """Set a string value for key with optional expiry in milliseconds."""
        expiry_ms = None
        if px is not None:
            expiry_ms = time.time_ns() // 1_000_000 + px
        self.store[key] = RedisEntry(type=RedisType.STRING, value=value, expiry_ms=expiry_ms)

    def _get_list(self, key: bytes) -> list[bytes] | None:
        """Return the list for key, or None if missing. Raises WrongTypeError if key holds a non-list."""
        entry = self._get_entry(key)
        if entry is None:
            return None
        if entry.type != RedisType.LIST:
            raise WrongTypeError
        assert isinstance(entry.value, list)
        return entry.value

    def rpush(self, key: bytes, value: list[bytes]) -> int:
        """Append values to the list at key. Creates the list if key doesn't exist. Raises WrongTypeError if key holds a string."""
        lst = self._get_list(key)
        if lst is None:
            self.store[key] = RedisEntry(type=RedisType.LIST, value=value[:])
        else:
            lst.extend(value)
        return len(self.store[key].value)  # type: ignore[arg-type]

    def lpush(self, key: bytes, value: list[bytes]) -> int:
        """Prepend values to the list at key. Creates the list if key doesn't exist. Raises WrongTypeError if key holds a string."""
        lst = self._get_list(key)
        if lst is None:
            self.store[key] = RedisEntry(type=RedisType.LIST, value=list(reversed(value)))
        else:
            lst[:0] = list(reversed(value))
        return len(self.store[key].value)  # type: ignore[arg-type]

    def llen(self, key: bytes) -> int:
        """Return the length of the list at key. Returns 0 if key doesn't exist. Raises WrongTypeError if key holds a string."""
        lst = self._get_list(key)
        return len(lst) if lst is not None else 0

    def lrange(self, key: bytes, start_index: int, end_index: int) -> list[bytes]:
        """Return values between start_index and end_index (inclusive) of the list at key."""
        lst = self._get_list(key)
        if lst is None:
            return []
        n = len(lst)

        if start_index < 0:
            start_index = n + start_index
        if end_index < 0:
            end_index = n + end_index

        start_index = max(0, start_index)
        end_index = min(end_index, n - 1)

        if start_index > end_index:
            return []
        return lst[start_index:end_index + 1]

    def lpop(self, key: bytes) -> bytes | None:
        lst = self._get_list(key)
        if lst is None:
            return None
        value = lst.pop(0)
        if not lst:
            del self.store[key]
        return value

    def rpop(self, key: bytes) -> bytes | None:
        lst = self._get_list(key)
        if lst is None:
            return None
        value = lst.pop()
        if not lst:
            del self.store[key]
        return value