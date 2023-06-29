package org.example;

import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;

import static org.example.Utils.compareKeys;

public class RocksdbKVStore implements KVStore {
    private final String db_path = "/home/e4r/test-database/rocksdb-java";
    private final Options options;
    private final Statistics statistics;
    private final RocksDB db;

    public RocksdbKVStore(RocksdbKVStoreConfig rocksdbKVStoreConfig) throws RocksDBException {
        RocksDB.loadLibrary();
        this.options = rocksdbKVStoreConfig.getOption();
        this.statistics = new Statistics();
        this.options.setStatistics(statistics);
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

    public void dumpStatistics(File file){
        try {
            Files.write(file.toPath(), this.options.statistics().toString().getBytes());
            System.out.println("Stats written to the file successfully.");
        } catch (IOException e) {
            System.err.println("Error writing stats to the file: " + e.getMessage());
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
            } while (kvPairs.hasNext());
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
