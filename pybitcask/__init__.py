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

    def __init__(self, key, value, timestamp=None, crc=None):
        self.key = key
        self.value = value
        self.key_size = len(key)
        self.value_size = len(value)
        self.timestamp = timestamp if timestamp is not None else int(time.time())
        self.crc = crc if crc is not None else crc32(self.value)

    def encode_entry(self):
        header = struct.pack(ENTRY_HEADER_FORMAT, self.crc,
                             self.timestamp, self.key_size, self.value_size)
        data = b"".join([self.key, self.value])
        return header + data

    def __repr__(self):
        return f"Entry (crc: {self.crc}, timestamp: {self.timestamp}, key_size: {self.key_size}, value_size: {self.value_size}, key: {self.key}, value: {self.value})"

    @staticmethod
    def decode_entry(entry_data):
        crc, timestamp, key_size, value_size = struct.unpack(
            ENTRY_HEADER_FORMAT, entry_data[:ENTRY_HEADER_SIZE])
        key_bytes = entry_data[ENTRY_HEADER_SIZE:ENTRY_HEADER_SIZE+key_size]
        value_bytes = entry_data[ENTRY_HEADER_SIZE+key_size:]
        assert crc == crc32(value_bytes), "CRC mismatch decoding entry"
        return Entry(key_bytes, value_bytes, timestamp=timestamp, crc=crc)

    @staticmethod
    def decode_header(header_data):
        crc, timestamp, key_size, value_size = struct.unpack(
            ENTRY_HEADER_FORMAT, header_data[:ENTRY_HEADER_SIZE])
        return crc, timestamp, key_size, value_size

    def __len__(self):
        return ENTRY_HEADER_SIZE + len(self.key) + len(self.value)

    def __eq__(self, other):
        if not isinstance(other, Entry):
            return False
        return (self.key == other.key and self.value == other.value
                and self.key_size == other.key_size
                and self.value_size == other.value_size
                and self.crc == other.crc
                and self.timestamp == other.timestamp)


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
            return KeyDir(pickle.load(f))

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
        # Set writepos to the end of any existing file data
        self.fp.seek(0, 2)  # SEEK_END
        self.writepos = self.fp.tell()

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
        self._ensure_write(data)
        self.writepos += len(entry)
        return pos_to_return

    def _ensure_write(self, data):
        self.fp.write(data)
        self.fp.flush()
        os.fsync(self.fp.fileno())

    def get(self, at_idx: int):
        self.fp.seek(at_idx, 0)
        header_data = self.fp.read(ENTRY_HEADER_SIZE)
        if not header_data or len(header_data) < ENTRY_HEADER_SIZE:
            return b""
        crc, timestamp, key_size, value_size = Entry.decode_header(header_data)
        key_bytes = self.fp.read(key_size)
        value_bytes = self.fp.read(value_size)
        # Verify CRC — if corrupted or partial, return empty
        if crc != crc32(value_bytes):
            return b""
        return value_bytes

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
            entry_start = self.fp.tell()  # position BEFORE the header
            header_data = self.fp.read(ENTRY_HEADER_SIZE)
            if not header_data:
                break
            crc, timestamp, key_size, value_size = Entry.decode_header(header_data)
            key_bytes = self.fp.read(key_size)
            value_bytes = self.fp.read(value_size)
            e = Entry(key_bytes, value_bytes, timestamp=timestamp, crc=crc)
            assert e.crc == crc
            yield e, entry_start


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
            f = removeprefix(file_path, f"{self.datadir}/").replace(".data", "")
            return int(f)

        def comp(el):
            return get_filename_as_int(el)

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
        # Sort by file_id so later (higher-numbered) files overwrite earlier ones
        sorted_datafiles = sorted(
            self.datafiles,
            key=lambda p: int(
                removeprefix(p, f"{self.datadir}/").replace(".data", ""))
        )
        for datafile_path in sorted_datafiles:
            datafile_id = removeprefix(
                datafile_path, f"{self.datadir}/").replace(".data", "")
            file_id = int(datafile_id)
            datafile = DataFile(file_id, self.datadir)
            for entry, entry_pos in datafile.entries:
                if entry.value == TOMBSTONE_VALUE:
                    keydir.delete(entry.key)
                    continue
                keydirentry = KeyDirEntry(datafile.file_id, len(entry),
                                          entry_pos, entry.timestamp, entry.key)
                keydir.put(entry.key, keydirentry)
            datafile.close()
        return keydir

    def get_activedatafile_id(self):
        return len(self.datafiles) + 1

    def get(self, key):
        if key not in self.keydir:
            return b""
        keydirentry = self.keydir.get(key)
        if keydirentry is None:
            return b""
        file_id = keydirentry.file_id
        if file_id == self.activedatafile.file_id:
            return self.activedatafile.get(keydirentry.entry_pos)
        else:
            datafile = DataFile(file_id, self.datadir)
            result = datafile.get(keydirentry.entry_pos)
            datafile.close()
            return result

    def put(self, key, value):
        entry = Entry(key, value)
        entry_pos = self.activedatafile.put(key, value)
        # Tombstone values are written to disk but NOT inserted into keydir;
        # the keydir is managed by delete() separately.
        if value != TOMBSTONE_VALUE:
            keydirentry = KeyDirEntry(
                self.activedatafile.file_id, len(entry), entry_pos,
                entry.timestamp, entry.key)
            self.keydir.put(key, keydirentry)
        if self.activedatafile.reached_max_size():
            self.activedatafile.close()
            self.datafiles.append(self.activedatafile.file_path)
            self.activedatafile = DataFile(
                self.get_activedatafile_id(), self.datadir)

    def delete(self, key):
        # Write tombstone first, then remove from keydir.
        # Order matters: if we crash between, a tombstone on disk is harmless.
        self.put(key, TOMBSTONE_VALUE)
        self.keydir.delete(key)

    def compact(self, to_dir_path=".bitcask"):
        os.makedirs(to_dir_path, exist_ok=True)
        # Close the active data file so its content is included in compaction
        self.activedatafile.close()
        if self.activedatafile.file_path not in self.datafiles and self.activedatafile.size() > 0:
            self.datafiles.append(self.activedatafile.file_path)
        # Reopen for any further writes
        self.activedatafile = DataFile(
            self.get_activedatafile_id(), self.datadir)

        # Build a single merged keydir from ALL data files (later files
        # override earlier ones; tombstones delete). This is the same logic
        # as _load_keydir.
        merged_keydir = self._load_keydir()

        if not merged_keydir.m:
            print("Compaction: nothing to compact")
            return

        # Write a single compacted data file + hint
        out_file_path = os.path.join(to_dir_path, "0001.data")
        out_hint_path = os.path.join(to_dir_path, "0001.hint")
        if os.path.exists(out_file_path):
            os.remove(out_file_path)
        if os.path.exists(out_hint_path):
            os.remove(out_hint_path)

        # Also clean up any stale numbered files from previous compactions
        for stale in glob.glob(f"{to_dir_path}/*.data"):
            os.remove(stale)
        for stale in glob.glob(f"{to_dir_path}/*.hint"):
            os.remove(stale)

        compact_keydir = KeyDir()
        datafile_out = DataFile(1, to_dir_path)
        for key, keydirentry in merged_keydir.m.items():
            # Read the value from the source data file
            source_file = DataFile(keydirentry.file_id, self.datadir)
            value = source_file.get(keydirentry.entry_pos)
            source_file.close()
            entry_pos = datafile_out.put(key, value)
            compact_keydir.put(
                key,
                KeyDirEntry(datafile_out.file_id, len(Entry(key, value)),
                            entry_pos, keydirentry.timestamp, key))
        datafile_out.close()
        compact_keydir.save_to_file(out_hint_path)
        print(f"Compaction done to {to_dir_path}")