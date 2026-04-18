"""Unit tests for RedisDB storage."""
import time
import pytest
from app.storage.redisdb import RedisDB


class TestRedisDB:
    """Test suite for RedisDB key-value store."""

    def test_set_and_get_basic(self):
        """Test basic set and get operations."""
        db = RedisDB()
        db.set(b"key1", b"value1")
        assert db.get(b"key1") == b"value1"

    def test_get_nonexistent_key(self):
        """Test getting a key that doesn't exist returns None."""
        db = RedisDB()
        assert db.get(b"nonexistent") is None

    def test_set_overwrites_existing_key(self):
        """Test that setting an existing key overwrites the value."""
        db = RedisDB()
        db.set(b"key1", b"value1")
        db.set(b"key1", b"value2")
        assert db.get(b"key1") == b"value2"

    def test_multiple_keys(self):
        """Test storing multiple different keys."""
        db = RedisDB()
        db.set(b"key1", b"value1")
        db.set(b"key2", b"value2")
        db.set(b"key3", b"value3")

        assert db.get(b"key1") == b"value1"
        assert db.get(b"key2") == b"value2"
        assert db.get(b"key3") == b"value3"

    def test_set_with_expiry_px(self):
        """Test setting a key with expiry in milliseconds (px parameter)."""
        db = RedisDB()
        db.set(b"temp_key", b"temp_value", px=100)  # 100ms expiry

        # Key should exist immediately
        assert db.get(b"temp_key") == b"temp_value"

        # Wait for expiry
        time.sleep(0.15)  # 150ms

        # Key should be expired and return None
        assert db.get(b"temp_key") is None

    def test_set_without_expiry(self):
        """Test that keys without expiry persist."""
        db = RedisDB()
        db.set(b"persistent_key", b"persistent_value")

        # Wait a bit
        time.sleep(0.1)

        # Key should still exist
        assert db.get(b"persistent_key") == b"persistent_value"

    def test_overwrite_removes_expiry(self):
        """Test that overwriting a key with expiry removes the expiry."""
        db = RedisDB()
        db.set(b"key1", b"value1", px=100)
        db.set(b"key1", b"value2")  # No expiry

        # Wait past original expiry
        time.sleep(0.15)

        # Key should still exist since new set had no expiry
        assert db.get(b"key1") == b"value2"

    def test_overwrite_with_new_expiry(self):
        """Test that overwriting a key with a new expiry updates the expiry time."""
        db = RedisDB()
        db.set(b"key1", b"value1", px=50)
        time.sleep(0.03)  # 30ms
        db.set(b"key1", b"value2", px=100)  # New 100ms expiry from now

        # Wait past original expiry but before new expiry
        time.sleep(0.05)  # Total: 80ms from first set, but only 50ms from second set

        # Key should still exist with new value
        assert db.get(b"key1") == b"value2"

    def test_expired_key_is_deleted_from_store(self):
        """Test that accessing an expired key deletes it from the internal store."""
        db = RedisDB()
        db.set(b"temp_key", b"temp_value", px=50)

        # Wait for expiry
        time.sleep(0.1)

        # Access the expired key (should trigger deletion)
        assert db.get(b"temp_key") is None

        # Verify it's actually deleted from internal store
        assert b"temp_key" not in db.store

    def test_empty_values(self):
        """Test storing empty byte strings."""
        db = RedisDB()
        db.set(b"empty_key", b"")
        assert db.get(b"empty_key") == b""

    def test_binary_values(self):
        """Test storing arbitrary binary data."""
        db = RedisDB()
        binary_data = b"\x00\x01\x02\xff\xfe"
        db.set(b"binary_key", binary_data)
        assert db.get(b"binary_key") == binary_data
