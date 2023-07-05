# rocksdb-bench

### Steps to run rocksdb-cpp stress test:

1. Install rocksdb libs:

```
    git clone https://github.com/facebook/rocksdb.git
    cd rocksdb
    DEBUG_LEVEL=0 make shared_lib install-shared
```

2. export rocksdb library path and run the stress test:

```
    cd rocksdb-bench
    export LD_LIBRARY_PATH=/usr/local/lib
    make bench
    ./build/cpp/benchmark1
```
