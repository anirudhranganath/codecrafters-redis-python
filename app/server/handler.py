import asyncio

from app.protocol import RedisProtocol
from app.storage import RedisDB

READ_BYTES_LIMIT = 4096


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    store: RedisDB,
) -> None:
    """Handle one TCP client connection.

    This coroutine is invoked by `asyncio.start_server(...)` for each accepted
    connection. It reads bytes from the socket and writes a RESP Simple String
    `+PONG\\r\\n` response for each received chunk.

    Args:
        reader: Async stream used to read bytes from the client.
        writer: Async stream used to write bytes back to the client.
    """
    addr = writer.get_extra_info("peername")
    print(f"Accepted connection from {addr}")

    try:
        while True:
            data = await reader.read(READ_BYTES_LIMIT)
            if not data:
                break

            print(f"Received data: {data!r}")
            response = await RedisProtocol.process_input(data, store)
            writer.write(response)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
