package org.example;

import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig;
import org.rocksdb.*;

import java.util.Iterator;
import java.util.List;

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

    @Override
    public void insertBatch(Iterator<KVPair> kvPairs) {
        try (final WriteBatch batch = new WriteBatch()) {
            while (kvPairs.hasNext()){
                KVPair kvPair = kvPairs.next();
                try {
                    batch.put(kvPair.key, kvPair.value);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                db.write(new WriteOptions(), batch);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<byte[]> readBatch(List<byte[]> keys) {
        try {
            return db.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }


}
