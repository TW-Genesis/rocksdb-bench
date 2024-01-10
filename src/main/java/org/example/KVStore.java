package org.example;

import java.util.Iterator;
import java.util.List;

public interface KVStore {
    void insert(byte[] key, byte[] value);
    byte[] find(byte[] key);
    void clean();

    void insertBatch(Iterator<KVPair> kvPairs,int batchSize);
    void readBatch(List<byte[]> keys);

    void rangeQuery(byte[] minKey, byte[] maxKey);
}
