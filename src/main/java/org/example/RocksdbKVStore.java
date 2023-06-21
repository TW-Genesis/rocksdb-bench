package org.example;

import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.example.Utils.compareKeys;

public class RocksdbKVStore implements KVStore{
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

    @Override
    public List<byte[]> rangeQuery(byte[] minKey, byte[] maxKey) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setFillCache(false); // Optional: Set cache behavior

        RocksIterator iterator = db.newIterator(readOptions);
        iterator.seek(minKey);

        ArrayList<byte[]> values = new ArrayList<>();
        while (iterator.isValid()) {
            byte[] key = iterator.key();
            if (compareKeys(key, minKey) >= 0 && compareKeys(key, maxKey) < 0) {
                byte[] value = iterator.value();
                values.add(value);
            }
            iterator.next();
        }

        iterator.close();
        return values;
    }


}
