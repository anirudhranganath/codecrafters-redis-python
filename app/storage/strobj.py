from dataclasses import dataclass
from enum import Enum


class RedisType(Enum):
    STRING = "string"
    LIST = "list"
    # SET = "set"   -> future
    # HASH = "hash" -> future


@dataclass
class RedisEntry:
    type: RedisType
    value: bytes | list[bytes]
    expiry_ms: int | None = None
