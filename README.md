# pybitcask

Implementation of the [bitcask paper](https://riak.com/assets/bitcask-intro.pdf)

**DON'T USE IN PRODUCTION** I'm not an expert in storage systems.

## installation

- `git clone github.com/xmonader/pybitcask`
- `poetry install`
- `poetry shell`
- to run the tests `make tests`


## Implemented

- [x] Entries encoding/decoding
- [x] Datafiles
- [x] Keydir
- [x] Offline compaction
- [] Inprocess compaction
- [] Locking dirs to only one process. always.

