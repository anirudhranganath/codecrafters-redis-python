import asyncio

PONG = b"+PONG\r\n"


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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
            data = await reader.read(4096)
            if not data:
                break

            print(f"Received data: {data!r}")
            writer.write(PONG)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

