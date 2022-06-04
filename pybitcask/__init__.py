from dataclasses import dataclass, field
from typing import *
import time
from binascii import crc32
import struct
import glob
import os
import pickle

def removeprefix(self: str, prefix: str, /) -> str:
    return self[len(prefix):] if self.startswith(prefix) else self[:]
        
def removesuffix(self: str, suffix: str, /) -> str:
    # suffix='' should not call self[:-0].
    return self[:-len(suffix)] if suffix and self.endswith(suffix) else self[:]


ENTRY_HEADER_FORMAT = "<LLLL"  # crc, timestamp, key_size, value_size
ENTRY_HEADER_SIZE = 16  # 4*4 bytes int.
TOMBSTONE_VALUE = b"$$T$$"
DATA_FILE_MAX_SIZE = 1024   # 1kb


@dataclass
class Entry:
    crc: int
    key_size: int
    value_size: int
    timestamp: int
    key: bytes
    value: bytes

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.key_size = len(key)
        self.value_size = len(value)
        # print(f"+++++KEY SIZE {self.key_size} VAL SIZE {self.value_size}")

        self.timestamp = int(time.time())
        self.crc = crc32(self.value)

    def encode_entry(self):
        header = struct.pack(ENTRY_HEADER_FORMAT, self.crc,
                             self.timestamp, self.key_size, self.value_size)
        data = b"".join([self.key, self.value])
        # print(f"ENCODING DATA, {self} and its size is ", ENTRY_HEADER_SIZE + len(data))
        return header + data

    def __repr__(self):
        return f"Entry (crc: {self.crc}, timestamp: {self.timestamp}, key_size: {self.key_size}, value_size: {self.value_size}, key: {self.key}, value: {self.value})"

    @staticmethod
    def decode_entry(entry_data):
        # print("TYPE: ", type(entry_data))
        crc, timestamp, key_size, value_size = struct.unpack(
            ENTRY_HEADER_FORMAT, entry_data[:ENTRY_HEADER_SIZE])
        key_bytes = entry_data[ENTRY_HEADER_SIZE:ENTRY_HEADER_SIZE+key_size]
        value_bytes = entry_data[ENTRY_HEADER_SIZE+key_size:]
        assert crc == crc32(value_bytes)
        return Entry(key_bytes, value_bytes)

    @staticmethod
    def decode_header(header_data):
        crc, timestamp, key_size, value_size = struct.unpack(
            ENTRY_HEADER_FORMAT, header_data[:ENTRY_HEADER_SIZE])
        # print(f"decoded HEADER: CRC {crc} {timestamp} {key_size} {value_size} ", "LEN HEADER: ", len(header_data))
        return crc, timestamp, key_size, value_size

    def __len__(self):
        return ENTRY_HEADER_SIZE + len(self.key) + len(self.value)


@dataclass
class KeyDirEntry:
    file_id: int
    entry_size: int
    entry_pos: int
    timestamp: int
    key: bytes


@dataclass
class KeyDir:
    def __init__(self, data=None):
        self.m: Dict[bytes, KeyDirEntry] = data or {}

    def put(self, key, keydirentry):
        self.m[key] = keydirentry

    def get(self, key):
        return self.m.get(key, None)

    def delete(self, key):
        return self.m.pop(key, None)

    def __contains__(self, key):
        return key in self.m

    def save_to_file(self, file_path):
        with open(file_path, "wb") as f:
            pickle.dump(self.m, f)

    @staticmethod
    def load_from_file(file_path):
        assert file_path.endswith(".hint")
        with open(file_path, "rb") as f:
            KeyDir(pickle.load(f))

    def merge(self, other):
        if other:
            self.m.update(other.m)

    def __repr__(self):
        return repr(self.m)


@dataclass
class DataFile:
    file_id: int

    def __init__(self, file_id, data_dir):
        self.data_dir = data_dir
        self.file_id = file_id
        self.fp = open(self.file_path, 'a+b')
        # print(f"FILEPATH {self.file_path}")
        self.writepos = 0

    @property
    def file_name(self):
        zfilled_id = f"{self.file_id}".zfill(4)
        return f"{zfilled_id}.data"

    @property
    def file_path(self):
        return os.path.join(self.data_dir, self.file_name)

    def put(self, key, value):
        entry = Entry(key, value)
        data = entry.encode_entry()
        pos_to_return = self.writepos
        # print(f"WRITING {data} of size {len(entry)}")
        self._ensure_write(data)
        self.writepos += len(entry)
        return pos_to_return

    def _ensure_write(self, data):

        self.fp.write(data)
        self.fp.flush()
        os.fsync(self.fp.fileno())

    def get(self, at_idx:int):
        self.fp.seek(at_idx, 0)
        # print(f"===AT IDX {at_idx}")
        header_data = self.fp.read(ENTRY_HEADER_SIZE)
        crc, timestamp, key_size, value_size = Entry.decode_header(
            header_data)
        # print("++++DATA: ", crc, timestamp, key_size, value_size)
        key_bytes = self.fp.read(key_size)
        # print(f"key: {key_bytes}, value: {value_bytes}")
        return self.fp.read(value_size)

    def size(self):
        return self.writepos

    def __len__(self):
        return self.size()

    def reached_max_size(self):
        return self.writepos >= DATA_FILE_MAX_SIZE

    def close(self):
        self.fp.flush()
        os.fsync(self.fp.fileno())
        self.fp.close()

    @property
    def entries(self):
        self.fp.seek(0)
        while True:
            header_data = self.fp.read(ENTRY_HEADER_SIZE)
            if not header_data:
                break
            crc, timestamp, key_size, value_size = Entry.decode_header(
                header_data)
            # print("LISTING ENTRIES DATA: ", crc, timestamp, key_size, value_size)
            key_bytes = self.fp.read(key_size)
            value_bytes = self.fp.read(value_size)
            e = Entry(key_bytes, value_bytes)
            e.timestamp = timestamp
            assert e.crc == crc
            yield e, self.fp.tell()


@dataclass
class Bitcask:
    activedatafile: DataFile
    datadir: str
    keydir: KeyDir
    datafiles: List[str] = field(default_factory=list)

    def __init__(self, datadir):
        self.datadir = datadir
        if not os.path.exists(datadir):
            os.makedirs(datadir, exist_ok=True)
        self.datafiles = glob.glob(f"{self.datadir}/*data")
        def get_filename_as_int(file_path):
            f =removeprefix(file_path, f"{self.datadir}/").replace(".data", "")
            return int(f)
        def comp(el): return get_filename_as_int(el) 
        self.datafiles.sort(key=comp, reverse=False)
        self.hints = glob.glob(f"{self.datadir}/*.hint")
        if self.hints:
            self.keydir = self._load_keydir_from_hints()
        else:
            self.keydir = self._load_keydir()

        self.activedatafile = DataFile(
            self.get_activedatafile_id(), self.datadir)

        print(f"INITED DB on {self.datadir}")

    def __repr__(self):
        return f"DATADIR: {self.datadir}"

    def _load_keydir_from_hints(self):
        globalkeydir = KeyDir()
        for file_path in glob.glob(f"{self.datadir}/*.hint"):
            keydir = KeyDir.load_from_file(file_path)
            globalkeydir.merge(keydir)

        return globalkeydir

    def _load_keydir(self, no_dead_values=False):

        keydir = KeyDir()
        for datafile_path in self.datafiles:

            datafile_id = removeprefix(datafile_path, f"{self.datadir}/").replace(".data", "")
            file_id = int(datafile_id)
            datafile = DataFile(file_id, self.datadir)
            for entry, entry_pos in datafile.entries:
                if no_dead_values and entry.value == TOMBSTONE_VALUE:
                    keydir.delete(entry.key)
                    continue
                keydirentry = KeyDirEntry(datafile.file_id, len(
                    entry), entry_pos, entry.timestamp, entry.key)
                keydir.put(entry.key, keydirentry)
            datafile.close()
        return keydir

    def get_activedatafile_id(self):

        return len(self.datafiles)+1

    def get(self, key):

        if key not in self.keydir:
            return ""  # TODO: how do we distinguish between empty value and non existing value?
        if keydirentry := self.keydir.get(key):
            file_id = keydirentry.file_id
            if file_id == self.activedatafile.file_id:
                return self.activedatafile.get(keydirentry.entry_pos)
            datafile = DataFile(file_id, self.datadir)
            return datafile.get(keydirentry.entry_pos)

    def put(self, key, value):
        entry = Entry(key, value)
        entry_data = entry.encode_entry()
        # print(f"AFTER ENCODING: entry_size {len(entry)}, entry_data {entry_data}")
        entry_pos = self.activedatafile.put(key, value)
        keydirentry = KeyDirEntry(int(self.get_activedatafile_id()), len(entry), entry_pos, entry.timestamp, entry.key)
        self.keydir.put(key, keydirentry)
        if self.activedatafile.reached_max_size():
            # print("REACHED MAX SIZE for file", self.activedatafile.file_path)
            self.activedatafile.close()
            self.datafiles.append(self.activedatafile.file_path)
            self.activedatafile = DataFile(int(self.get_activedatafile_id()), self.datadir)
            # print("OPENED NEW DATA FILE", self.activedatafile.file_path)

    def delete(self, key):
        self.keydir.delete(key)
        self.put(key, TOMBSTONE_VALUE)

    def compact(self, to_dir_path=".bitcask"):
        os.makedirs(to_dir_path, exist_ok=True)
        for datafile in self.datafiles:
            keydir = KeyDir()
            datafile_id = removeprefix(datafile, f"{self.datadir}/").replace(".data", "")
            file_id = int(datafile_id)
            datafile_in = DataFile(file_id, self.datadir)
            datafile_out = DataFile(file_id, to_dir_path)
            for entry, _ in datafile_in.entries:
                if entry.value == TOMBSTONE_VALUE:
                    keydir.delete(entry.key)
                    continue
                entry_pos = datafile_out.put(entry.key, entry.value)
                keydirentry = KeyDirEntry(datafile_in.file_id, len(
                    entry), entry_pos, entry.timestamp, entry.key)
                keydir.put(entry.key, keydirentry)
            datafile_in.close()
            datafile_out.close()
            keydir.save_to_file(f"{to_dir_path}/{str(datafile_out.file_id).zfill(4)}.hint")
