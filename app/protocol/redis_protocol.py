from enum import Enum
from typing import Callable

from app.storage import RedisDB
from app.storage.redisdb import WrongTypeError

WRONGTYPE_ERROR = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

_CRLF = b"\r\n"


def _bulk(value: bytes) -> bytes:
    return f"${len(value)}\r\n".encode() + value + _CRLF


def _handle_ping(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) == 1:
        return b"+PONG\r\n"
    return _bulk(args[1])


def _handle_echo(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) != 2:
        return b"-ERR wrong number of arguments for 'echo' command\r\n"
    return _bulk(args[1])


def _handle_set(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for 'set' command\r\n"
    key, value = args[1], args[2]
    px = None
    if len(args) == 5:
        option = args[3].upper()
        try:
            expiry_val = int(args[4])
            if option == b"EX":
                px = expiry_val * 1000
            elif option == b"PX":
                px = expiry_val
            else:
                return b"-ERR syntax error\r\n"
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
    elif len(args) != 3:
        return b"-ERR syntax error\r\n"
    store.set(key, value, px=px)
    return b"+OK\r\n"


def _handle_get(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) != 2:
        return b"-ERR wrong number of arguments for 'get' command\r\n"
    try:
        value = store.get(args[1])
    except WrongTypeError:
        return WRONGTYPE_ERROR
    if value is None:
        return b"$-1\r\n"
    return _bulk(value)


def _handle_rpush(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for 'rpush' command\r\n"
    try:
        items = store.rpush(args[1], args[2:])
    except WrongTypeError:
        return WRONGTYPE_ERROR
    return f":{items}\r\n".encode()


def _handle_lpush(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for 'lpush' command\r\n"
    try:
        items = store.lpush(args[1], args[2:])
    except WrongTypeError:
        return WRONGTYPE_ERROR
    return f":{items}\r\n".encode()


def _handle_lrange(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) != 4:
        return b"-ERR wrong number of arguments for 'lrange' command\r\n"
    try:
        start, end = int(args[2]), int(args[3])
        items = store.lrange(args[1], start, end)
    except WrongTypeError:
        return WRONGTYPE_ERROR
    except ValueError:
        return b"-ERR value is not an integer or out of range\r\n"
    ret_string = b"".join(_bulk(item) for item in items)
    return f"*{len(items)}\r\n".encode() + ret_string

def _handle_llen(args: list[bytes], store: RedisDB) -> bytes:
    if len(args) != 2:
        return b"-ERR wrong number of arguments for 'llen' command\r\n"
    try:
        length = store.llen(args[1])
    except WrongTypeError:
        return WRONGTYPE_ERROR
    return f":{length}\r\n".encode()

class Command(str, Enum):
    """Redis command types."""

    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    LPUSH = "LPUSH"
    RPUSH = "RPUSH"
    LRANGE = "LRANGE"
    LLEN = "LLEN"


_HANDLERS: dict[Command, Callable[[list[bytes], RedisDB], bytes]] = {
    Command.PING:   _handle_ping,
    Command.ECHO:   _handle_echo,
    Command.SET:    _handle_set,
    Command.GET:    _handle_get,
    Command.RPUSH:  _handle_rpush,
    Command.LPUSH:  _handle_lpush,
    Command.LRANGE: _handle_lrange,
    Command.LLEN: _handle_llen,
}

if set(_HANDLERS) != set(Command):
    raise RuntimeError(f"Unhandled commands: {set(Command) - set(_HANDLERS)}")


class RedisProtocol:
    RESP_ARRAY_PREFIX = b"*"
    RESP_BULK_STRING_PREFIX = b"$"
    RESP_SIMPLE_STRING_PREFIX = b"+"
    RESP_ERROR_PREFIX = b"-"
    RESP_INTEGER_PREFIX = b":"

    CRLF = _CRLF

    @classmethod
    async def process_input(cls, data: bytes, store: RedisDB) -> bytes:
        """Process the input command and return the output data."""
        first = data[:1]
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
        """Parse a RESP array and dispatch to the appropriate command handler.

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

        try:
            handler = _HANDLERS[Command(args[0].upper().decode())]
        except (ValueError, KeyError):
            return b"-ERR unknown command\r\n"
        return handler(args, store)

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
