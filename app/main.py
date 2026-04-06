import asyncio

from app.server.handler import handle_client

HOST = "localhost"
PORT = 6379

async def start_server() -> None:
    """Start the asyncio TCP server and serve forever.

    Binds to (`HOST`, `PORT`) and dispatches each client connection to
    `app.server.handler.handle_client`.
    """
    server = await asyncio.start_server(handle_client, HOST, PORT)
    # (Optional) print where it's listening
    sockets = server.sockets or []
    for s in sockets:
        print(f"Listening on {s.getsockname()}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    loop = asyncio.run(start_server())
