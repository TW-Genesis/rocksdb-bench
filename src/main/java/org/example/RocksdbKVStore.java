package org.example;

import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksdbKVStore implements KVStore{
    private final String db_path = "/tmp/rocksdb-java";
    private final Options options;
    private final RocksDB db;

    public RocksdbKVStore(RocksdbKVStoreConfig rocksdbKVStoreConfig) throws RocksDBException {
        RocksDB.loadLibrary();
        this.options = rocksdbKVStoreConfig.getOption();
        this.db = RocksDB.open(this.options, db_path);
    }

    @Override
    public void insert(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] find(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clean() {
        db.close();
        try {
            RocksDB.destroyDB(db_path, options);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

}
