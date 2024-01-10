package org.example;

import org.apache.jena.dboe.base.file.FileSet;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.dboe.trans.bplustree.BPlusTreeFactory;
import org.apache.jena.dboe.transaction.txn.ComponentId;

import java.util.Iterator;
import java.util.List;

public class JenaBPTKVStore implements KVStore {
    private final String BPT_dir = "/home/e4r/test-database/jena-bpt";
    private final BPlusTree bPlusTree;
    private final int keyLength = 24;
    private final int valueLength = 0;

    public JenaBPTKVStore() {
        this.bPlusTree = BPlusTreeFactory.createBPTree(ComponentId.allocLocal(), new FileSet(BPT_dir, "bptree-java"), new RecordFactory(keyLength, valueLength));
        this.bPlusTree.nonTransactional();
    }

    @Override
    public void insert(byte[] key, byte[] value) {
        bPlusTree.insert(new Record(key, value));
    }

    @Override
    public byte[] find(byte[] key) {
        return bPlusTree.find(new Record(key, null)).getValue();
    }

    @Override
    public void clean() {
        bPlusTree.clear();
        bPlusTree.close();
    }

    @Override
    public void insertBatch(Iterator<KVPair> kvPairs, int batchSize) {
        while (kvPairs.hasNext()) {
            KVPair kvPair = kvPairs.next();
            this.insert(kvPair.key, kvPair.value);
        }
    }

    @Override
    public void readBatch(List<byte[]> keys) {
        for (int i = 0; i < keys.size(); i++) {
            this.find(keys.get(i));
        }
    }

    @Override
    public void rangeQuery(byte[] minKey, byte[] maxKey) {
        Iterator<Record> iterator = bPlusTree.iterator(new Record(minKey, null), new Record(maxKey, null));

        while (iterator.hasNext()) {
            byte[] key = iterator.next().getKey();
        }
    }
}
