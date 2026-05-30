import pybitcask as bc
import os
import shutil
import tempfile


def test_create_entry():
    e = bc.Entry(b'key', b'value')
    assert e.key == b'key'
    assert e.value == b'value'
    assert e.key_size == len(b'key')
    assert e.value_size == len(b'value')


def test_encode_decode_entry():
    e = bc.Entry(b'key', b'value')
    encoded = e.encode_entry()
    assert len(e) == 16 + e.key_size + e.value_size
    decoded = bc.Entry.decode_entry(encoded)
    assert e == decoded


def test_put_get():
    """Basic put/get: a written key reads back its value."""
    dbdir = tempfile.mkdtemp()
    try:
        b = bc.Bitcask(dbdir)
        b.put(b'hello', b'world')
        assert b.get(b'hello') == b'world'
        # Non-existent key returns empty bytes
        assert b.get(b'nope') == b''
    finally:
        shutil.rmtree(dbdir, ignore_errors=True)


def test_overwrite():
    """Overwriting a key returns the latest value."""
    dbdir = tempfile.mkdtemp()
    try:
        b = bc.Bitcask(dbdir)
        b.put(b'key', b'v1')
        assert b.get(b'key') == b'v1'
        b.put(b'key', b'v2')
        assert b.get(b'key') == b'v2'
        b.put(b'key', b'v3')
        assert b.get(b'key') == b'v3'
    finally:
        shutil.rmtree(dbdir, ignore_errors=True)


def test_delete():
    """A deleted key reads back as empty (b''), not the tombstone."""
    dbdir = tempfile.mkdtemp()
    try:
        b = bc.Bitcask(dbdir)
        b.put(b'key', b'value')
        assert b.get(b'key') == b'value'
        b.delete(b'key')
        assert b.get(b'key') == b''

        # Deleting a never-set key also returns empty
        b.delete(b'ghost')
        assert b.get(b'ghost') == b''
    finally:
        shutil.rmtree(dbdir, ignore_errors=True)


def test_persistence():
    """After closing and reopening, live keys read correctly and deleted keys stay deleted."""
    dbdir = tempfile.mkdtemp()
    try:
        # First session: write some keys and delete some
        b = bc.Bitcask(dbdir)
        for i in range(50):
            b.put(f"key_{i}".encode(), f"value_{i}".encode())
        # Delete first 10 keys
        for i in range(10):
            b.delete(f"key_{i}".encode())
        # Overwrite some keys
        for i in range(20, 30):
            b.put(f"key_{i}".encode(), f"overwritten_{i}".encode())

        # Verify before close
        for i in range(10):
            assert b.get(f"key_{i}".encode()) == b"", f"key_{i} should be deleted"
        for i in range(10, 20):
            assert b.get(f"key_{i}".encode()) == f"value_{i}".encode()
        for i in range(20, 30):
            assert b.get(f"key_{i}".encode()) == f"overwritten_{i}".encode()
        for i in range(30, 50):
            assert b.get(f"key_{i}".encode()) == f"value_{i}".encode()

        # Close and reopen
        del b

        b2 = bc.Bitcask(dbdir)
        # Verify all keys after reopen
        for i in range(10):
            assert b2.get(f"key_{i}".encode()) == b"", f"reopen: key_{i} should be deleted"
        for i in range(10, 20):
            assert b2.get(f"key_{i}".encode()) == f"value_{i}".encode()
        for i in range(20, 30):
            assert b2.get(f"key_{i}".encode()) == f"overwritten_{i}".encode()
        for i in range(30, 50):
            assert b2.get(f"key_{i}".encode()) == f"value_{i}".encode()

        # Non-existent key
        assert b2.get(b'never_set') == b''
    finally:
        shutil.rmtree(dbdir, ignore_errors=True)


def test_file_rotation():
    """Many writes across small data files: every key still reads correctly."""
    dbdir = tempfile.mkdtemp()
    try:
        # DATA_FILE_MAX_SIZE is 1024 bytes, so ~40-50 small entries should
        # trigger rotation into multiple data files.
        b = bc.Bitcask(dbdir)
        N = 200
        for i in range(N):
            b.put(f"rot_key_{i:04d}".encode(), f"rot_value_{i:04d}".encode())

        # Verify all keys
        for i in range(N):
            key = f"rot_key_{i:04d}".encode()
            expected = f"rot_value_{i:04d}".encode()
            assert b.get(key) == expected, f"rotation: {key} mismatch"

        # Delete some keys across files
        for i in range(0, N, 10):
            b.delete(f"rot_key_{i:04d}".encode())

        # Verify deletions
        for i in range(0, N, 10):
            assert b.get(f"rot_key_{i:04d}".encode()) == b"", f"rot deletion: key_{i}"

        # Verify non-deleted keys still intact
        for i in range(N):
            if i % 10 != 0:
                key = f"rot_key_{i:04d}".encode()
                expected = f"rot_value_{i:04d}".encode()
                assert b.get(key) == expected, f"rotation: {key} mismatch after deletes"

        # Reopen and verify everything
        del b
        b2 = bc.Bitcask(dbdir)
        for i in range(0, N, 10):
            assert b2.get(f"rot_key_{i:04d}".encode()) == b""
        for i in range(N):
            if i % 10 != 0:
                key = f"rot_key_{i:04d}".encode()
                expected = f"rot_value_{i:04d}".encode()
                assert b2.get(key) == expected, f"reopen rotation: {key}"
    finally:
        shutil.rmtree(dbdir, ignore_errors=True)


def test_compaction():
    """Compaction keeps live keys and drops deleted ones."""
    dbdir = tempfile.mkdtemp()
    compact_dir = tempfile.mkdtemp()
    try:
        b = bc.Bitcask(dbdir)
        for i in range(100):
            b.put(f"ckey_{i:03d}".encode(), f"cval_{i:03d}".encode())

        # Delete every 3rd key
        deleted = set()
        for i in range(0, 100, 3):
            b.delete(f"ckey_{i:03d}".encode())
            deleted.add(i)

        # Overwrite some keys (only those not deleted)
        for i in range(10, 30):
            if i not in deleted:
                b.put(f"ckey_{i:03d}".encode(), f"cval_overwritten_{i:03d}".encode())

        # Compact
        b.compact(compact_dir)

        # Reopen from compacted directory
        b2 = bc.Bitcask(compact_dir)

        # Deleted keys must be gone
        for i in deleted:
            assert b2.get(f"ckey_{i:03d}".encode()) == b"", f"compact: deleted key ckey_{i:03d} should be gone"

        # Live keys must be present with correct values
        for i in range(100):
            if i not in deleted:
                if 10 <= i < 30:
                    expected = f"cval_overwritten_{i:03d}".encode()
                else:
                    expected = f"cval_{i:03d}".encode()
                actual = b2.get(f"ckey_{i:03d}".encode())
                assert actual == expected, f"compact: key ckey_{i:03d} expected {expected!r} got {actual!r}"
    finally:
        shutil.rmtree(dbdir, ignore_errors=True)
        shutil.rmtree(compact_dir, ignore_errors=True)