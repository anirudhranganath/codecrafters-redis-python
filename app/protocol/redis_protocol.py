from enum import Enum

from app.storage import RedisDB


class Command(str, Enum):
    """Redis command types."""

    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"


class RedisProtocol:
    RESP_ARRAY_PREFIX = b"*"
    RESP_BULK_STRING_PREFIX = b"$"
    RESP_SIMPLE_STRING_PREFIX = b"+"
    RESP_ERROR_PREFIX = b"-"
    RESP_INTEGER_PREFIX = b":"

    CRLF = b"\r\n"

    @classmethod
    async def process_input(cls, data: bytes, store: RedisDB) -> bytes:
        """Process the input command and return the output data."""
        first = data[:1]  # b'*', b'$', etc. (safe even if empty)
        if first == cls.RESP_ARRAY_PREFIX:
            return cls.process_array(data, store)
        if first == cls.RESP_BULK_STRING_PREFIX:
            return cls.process_bulk_string(data)
        if first == cls.RESP_SIMPLE_STRING_PREFIX:
            return cls.process_simple_string(data)
        if first == cls.RESP_ERROR_PREFIX:
            return cls.process_error(data)
        if first == cls.RESP_INTEGER_PREFIX:
            return cls.process_integer(data)
        return b"-ERR unknown command\r\n"

    @classmethod
    def process_array(cls, data: bytes, store: RedisDB) -> bytes:
        """Handle RESP array payload.
        
        Arrays are of the form *<number-of-elements>\r\n<element-1>...<element-n>   
        """
        try:
            line_end = data.find(cls.CRLF, 1)
            if line_end == -1:
                return b"-ERR protocol error\r\n"
            length = int(data[1:line_end])
            if length < 0:
                return b"-ERR protocol error\r\n"

            pos = line_end + 2
            args: list[bytes] = []
            for _ in range(length):
                value, pos = cls.process_bulk_string(data, pos)
                args.append(value)
        except (ValueError, IndexError):
            return b"-ERR protocol error\r\n"

        if pos != len(data):
            return b"-ERR protocol error\r\n"
        if not args:
            return b"-ERR unknown command\r\n"

        command = args[0].upper()
        if command == Command.PING.value.encode():
            if len(args) == 1:
                return b"+PONG\r\n"
            if len(args) > 1:
                payload = args[1]
                return f"${len(payload)}\r\n".encode() + payload + cls.CRLF
            return b"-ERR wrong number of arguments for 'ping' command\r\n"

        if command == Command.ECHO.value.encode():
            if len(args) != 2:
                return b"-ERR wrong number of arguments for 'echo' command\r\n"
            payload = args[1]
            return f"${len(payload)}\r\n".encode() + payload + cls.CRLF

        if command == Command.SET.value.encode():
            if len(args) != 3:
                return b"-ERR wrong number of arguments for 'set' command\r\n"
            key, value = args[1], args[2]
            store.set(key, value)
            return b"+OK\r\n"

        if command == Command.GET.value.encode():
            if len(args) != 2:
                return b"-ERR wrong number of arguments for 'get' command\r\n"
            key = args[1]
            value = store.get(key)
            if value is None:
                return b"$-1\r\n"
            return f"${len(value)}\r\n".encode() + value + cls.CRLF

        return b"-ERR unknown command\r\n"


    @classmethod
    def process_bulk_string(cls, data: bytes, pos: int | None = None) -> tuple[bytes, int] | bytes:
        """Parse a bulk string from `data` at `pos` and return `(value, next_pos)`.

        When called without `pos`, this acts as a top-level dispatcher stub.
        """
        if pos is None:
            return b"-ERR bulk string parsing not implemented\r\n"

        if data[pos:pos + 1] != cls.RESP_BULK_STRING_PREFIX:
            raise ValueError("expected bulk prefix")
        bulk_end = data.find(cls.CRLF, pos + 1)
        if bulk_end == -1:
            raise ValueError("missing bulk length terminator")
        bulk_len = int(data[pos + 1:bulk_end])
        if bulk_len < 0:
            raise ValueError("null bulk not allowed")

        start = bulk_end + 2
        end = start + bulk_len
        if data[end:end + 2] != cls.CRLF:
            raise ValueError("missing bulk data terminator")
        return data[start:end], end + 2

    @staticmethod
    def process_simple_string(data: bytes) -> bytes:
        """Handle RESP simple string payload."""
        _ = data
        return b"-ERR simple string parsing not implemented\r\n"

    @staticmethod
    def process_error(data: bytes) -> bytes:
        """Handle RESP error payload."""
        _ = data
        return b"-ERR error parsing not implemented\r\n"

    @staticmethod
    def process_integer(data: bytes) -> bytes:
        """Handle RESP integer payload."""
        _ = data
        return b"-ERR integer parsing not implemented\r\n"

