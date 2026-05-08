import asyncio
import time

from app.storage.strobj import RedisEntry, RedisType


class WrongTypeError(Exception):
    """Raised when an operation is performed on a key holding the wrong type."""


class RedisDB:
    """In-memory key-value store (Redis-style bulk strings as bytes)."""

    def __init__(self):
        self.store: dict[bytes, RedisEntry] = {}
        self._block_waiters: dict[bytes, list[asyncio.Event]] = {}

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
        # Notify one waiter per element added
        for _ in range(len(value)):
            self._notify_waiters(key)
        return len(self.store[key].value)  # type: ignore[arg-type]

    def lpush(self, key: bytes, value: list[bytes]) -> int:
        """Prepend values to the list at key. Creates the list if key doesn't exist. Raises WrongTypeError if key holds a string."""
        lst = self._get_list(key)
        if lst is None:
            self.store[key] = RedisEntry(type=RedisType.LIST, value=list(reversed(value)))
        else:
            lst[:0] = list(reversed(value))
        # Notify one waiter per element added
        for _ in range(len(value)):
            self._notify_waiters(key)
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

    def lpop(self, key: bytes, count: int | None = None) -> bytes | list[bytes] | None:
        lst = self._get_list(key)
        if lst is None:
            return None
        if count is None:
            value = lst.pop(0)
            if not lst:
                del self.store[key]
            return value
        count = min(count, len(lst))
        values, lst[:count] = lst[:count], []
        if not lst:
            del self.store[key]
        return values

    def rpop(self, key: bytes, count: int | None = None) -> bytes | list[bytes] | None:
        lst = self._get_list(key)
        if lst is None:
            return None
        if count is None:
            value = lst.pop()
            if not lst:
                del self.store[key]
            return value
        count = min(count, len(lst))
        values = lst[-count:]
        del lst[-count:]
        if not lst:
            del self.store[key]
        return values

    def _notify_waiters(self, key: bytes) -> None:
        """Notify one waiter blocked on this key (FIFO order)."""
        if key in self._block_waiters and self._block_waiters[key]:
            # Only wake the first waiter (FIFO)
            event = self._block_waiters[key][0]
            event.set()

    async def blpop(self, keys: list[bytes], timeout_seconds: float) -> tuple[bytes, bytes] | None:
        """Block until an element is available in one of the keys or timeout.

        Returns (key, value) tuple or None on timeout.
        """
        start_time = asyncio.get_event_loop().time()

        while True:
            # Try to pop from any existing non-empty list
            for key in keys:
                try:
                    lst = self._get_list(key)
                    if lst and len(lst) > 0:
                        value = lst.pop(0)
                        if not lst:
                            del self.store[key]
                        return (key, value)
                except WrongTypeError:
                    # Key exists but is wrong type - skip it
                    continue

            # No data available, need to block
            event = asyncio.Event()

            # Register this waiter for all keys
            for key in keys:
                if key not in self._block_waiters:
                    self._block_waiters[key] = []
                self._block_waiters[key].append(event)

            try:
                # Calculate remaining timeout
                if timeout_seconds == 0:
                    # 0 means block indefinitely
                    remaining_timeout = None
                else:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_timeout = timeout_seconds - elapsed
                    if remaining_timeout <= 0:
                        return None

                # Wait for either timeout or notification
                if remaining_timeout is None:
                    await event.wait()
                else:
                    try:
                        await asyncio.wait_for(event.wait(), timeout=remaining_timeout)
                    except asyncio.TimeoutError:
                        return None

                # We were notified, loop back to try popping again
                # This handles race conditions where another waiter got the data

            finally:
                # Clean up waiter registration
                for key in keys:
                    if key in self._block_waiters:
                        try:
                            self._block_waiters[key].remove(event)
                            if not self._block_waiters[key]:
                                del self._block_waiters[key]
                        except (ValueError, KeyError):
                            pass