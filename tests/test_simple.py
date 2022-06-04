import pybitcask as bc
import os
import shutil

def test_true():
    assert True is True

def test_create_entry():
    e = bc.Entry(b'key', b'value')
    assert e.key == b'key'
    assert e.value == b'value'
    assert e.key_size == len(b'key')
    assert e.value_size == len(b'value')


def test_encode_decode_entry():
    e = bc.Entry(b'key', b'value')
    assert e.key == b'key'
    assert e.value == b'value'
    assert e.key_size == len(b'key')
    assert e.value_size == len(b'value')

    encoded = e.encode_entry()
    assert len(e) == 16 + e.key_size+e.value_size

    decoded = bc.Entry.decode_entry(encoded)
    assert e == decoded


def test_kv_simple():
    DBPATH = "/tmp/bctestdb"
    N_KEYS = 100
    b = bc.Bitcask(DBPATH)
    for i in range(N_KEYS):
        b.put(f"key__{i}".encode(), f"value__{i}".encode())

    print(b.keydir)
    for z in range(N_KEYS):
        print("Z: ",z)
        print("val of ", "key__",z, "is ", b.get(f"key__{z}".encode()))
        # assert b.get(f"key__{i}".encode()) ==  f"value__{i}".encode()


    for k in range(10):
        print(f"deleting key__{k}".encode())
        b.delete(f"key__{k}".encode())


    b.compact(f"{DBPATH}/bitcask")
    b2 = bc.Bitcask(DBPATH + "/bitcask") # after compaction


    for k in range(10):
        print(f"checking for deleted key__{k} after compaction".encode())
        assert b2.get(f"key__{k}".encode()) == ""    

    for k in range(10):
        print(f"checking for key key__{k} before compaction".encode())
        assert b.get(f"key__{k}".encode()) == bc.TOMBSTONE_VALUE    
    shutil.rmtree(DBPATH)
# def test_put_get_key():
#     b = bc.Bitcask('test.db')
#     b.put(b'key', b'value')
#     assert b.get(b'key') == b'value'