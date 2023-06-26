package org.example;

import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig;
import org.rocksdb.*;

import java.util.Iterator;
import java.util.List;

import static org.example.Utils.compareKeys;

public class RocksdbKVStore implements KVStore {
    private final String db_path = "/home/e4r/test-database/rocksdb-java";
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
    public void insertBatch(Iterator<KVPair> kvPairs, int batchSize) {
        try {
            int noOfBatches = Utils.keyValueCount(kvPairs) / batchSize;
            for (int batchNumber = 0; batchNumber < noOfBatches; batchNumber++) {
                int batchKVPairs = 0;
                WriteBatch batch = new WriteBatch();
                while (kvPairs.hasNext() && batchKVPairs <= batchSize) {
                    KVPair kvPair = kvPairs.next();
                    try {
                        batch.put(kvPair.key, kvPair.value);
                        batchKVPairs++;
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
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readBatch(List<byte[]> keys) {
        try {
            db.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rangeQuery(byte[] minKey, byte[] maxKey) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setFillCache(false); // Optional: Set cache behavior

        RocksIterator iterator = db.newIterator(readOptions);
        iterator.seek(minKey);

        while (iterator.isValid()) {
            byte[] key = iterator.key();
            if (compareKeys(key, minKey) >= 0 && compareKeys(key, maxKey) < 0) {
                byte[] value = iterator.value();
            }
            iterator.next();
        }

        iterator.close();
    }


}
