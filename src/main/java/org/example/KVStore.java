package org.example;

public interface KVStore {
    void insert(byte[] key, byte value[]);
    byte[] find(byte[] key);
    void clean();
}
