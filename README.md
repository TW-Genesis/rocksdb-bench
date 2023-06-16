git clone https://github.com/facebook/rocksdb.git
cd rocksdb

DEBUG_LEVEL=0 make shared_lib install-shared

export LD_LIBRARY_PATH=/usr/local/lib
# rocksdb-bench
