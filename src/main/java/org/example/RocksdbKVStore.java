package org.example;

import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig;
import org.rocksdb.*;

import java.util.Iterator;
import java.util.List;

import static org.example.Utils.compareKeys;

public class RocksdbKVStore implements KVStore {
    private final String db_path = "/home/e4r/test-database/rocksdb-java";
    private final Options options;
    private final Statistics statistics;
    private final RocksDB rocksDB;

    public RocksdbKVStore(RocksdbKVStoreConfig rocksdbKVStoreConfig) throws RocksDBException {
        RocksDB.loadLibrary();
        this.options = rocksdbKVStoreConfig.getOption();
        this.statistics = new Statistics();
        this.options.setStatistics(statistics);
        this.options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
        this.rocksDB = RocksDB.open(this.options, db_path);
    }

    @Override
    public void insert(byte[] key, byte[] value) {
        try {
            if (value == null) {
                value = new byte[0];
            }
            rocksDB.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] find(byte[] key) {
        try {
            byte[] value = rocksDB.get(key);
            if (value.length == 0) {
                value = null;
            }
            return value;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clean() {
        rocksDB.close();
        try {
            RocksDB.destroyDB(db_path, options);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertBatch(Iterator<KVPair> kvPairs, int batchSize) {
        try {
            do {
                int batchKVPairs = 0;
                WriteBatch batch = new WriteBatch();
                while (kvPairs.hasNext() && batchKVPairs < batchSize) {
                    KVPair kvPair = kvPairs.next();
                    try {
                        if (kvPair.value == null)
                            kvPair.value = new byte[0];
                        batch.put(kvPair.key, kvPair.value);
                        batchKVPairs++;
                    } catch (RocksDBException e) {
                        batch.close();
                        throw new RuntimeException(e);
                    }
                }
                try {
                    rocksDB.write(new WriteOptions(), batch);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            } while (kvPairs.hasNext());
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readBatch(List<byte[]> keys) {
        try {
            rocksDB.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rangeQuery(byte[] minKey, byte[] maxKey) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setFillCache(false); // Optional: Set cache behavior

        RocksIterator iterator = rocksDB.newIterator(readOptions);
        iterator.seek(minKey);

        while (iterator.isValid()) {
            byte[] key = iterator.key();

            if (compareKeys(key, maxKey) < 0) {
                byte[] value = iterator.value();
                if (value.length == 0) {
                    value = null;
                }
            } else {
                break;
            }
            iterator.next();
        }
        iterator.close();
    }
}
