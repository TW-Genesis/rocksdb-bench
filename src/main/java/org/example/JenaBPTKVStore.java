package org.example;

import org.apache.jena.dboe.base.file.FileSet;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.dboe.trans.bplustree.BPlusTreeFactory;
import org.apache.jena.dboe.transaction.txn.ComponentId;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig;

public class JenaBPTKVStore implements KVStore{
    private final String BPT_dir = "/tmp/jena-bpt";
    private final BPlusTree bPlusTree;

    public JenaBPTKVStore(JenaBPTKVStoreConfig jenaBPTKVStoreConfig) {
        this.bPlusTree = BPlusTreeFactory.createBPTree(ComponentId.allocLocal(), new FileSet(BPT_dir, "bptree-java"), new RecordFactory(jenaBPTKVStoreConfig.getKVSize(), jenaBPTKVStoreConfig.getKVSize()));
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
    }


}
