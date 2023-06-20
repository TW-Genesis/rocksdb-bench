package org.example;

import java.util.Iterator;
import java.util.List;

public interface KVStore {
    void insert(byte[] key, byte value[]);
    byte[] find(byte[] key);
    void clean();

    void insertBatch(Iterator<KVPair> kvPairs);
    List<byte[]> readBatch(List<byte[]> keys);
}
