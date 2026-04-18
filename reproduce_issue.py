import asyncio
import time
from app.storage import RedisDB
from app.protocol import RedisProtocol

async def test_set_expiry():
    store = RedisDB()
    
    # Test EX (seconds)
    print("Testing SET key value EX 1...")
    # *5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n
    cmd_ex = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n"
    resp = await RedisProtocol.process_input(cmd_ex, store)
    assert resp == b"+OK\r\n"
    assert store.get(b"mykey") == b"value"
    print("Key exists immediately after SET.")
    
    time.sleep(1.1)
    assert store.get(b"mykey") is None
    print("Key expired after 1.1s.")

    # Test PX (milliseconds)
    print("\nTesting SET key value PX 500...")
    # *5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n500\r\n
    cmd_px = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n500\r\n"
    resp = await RedisProtocol.process_input(cmd_px, store)
    assert resp == b"+OK\r\n"
    assert store.get(b"mykey") == b"value"
    print("Key exists immediately after SET.")
    
    time.sleep(0.6)
    assert store.get(b"mykey") is None
    print("Key expired after 0.6s.")

    print("\nAll tests passed!")

if __name__ == "__main__":
    asyncio.run(test_set_expiry())
