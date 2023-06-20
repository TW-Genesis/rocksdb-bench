package org.example;

import org.apache.jena.dboe.base.file.FileSet;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.dboe.trans.bplustree.BPlusTreeFactory;
import org.apache.jena.dboe.transaction.txn.ComponentId;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    @Override
    public void insertBatch(Iterator<KVPair> kvPairs) {
        while (kvPairs.hasNext()){
            KVPair kvPair = kvPairs.next();
            bPlusTree.insert(new Record(kvPair.key, kvPair.value));
        }
    }

    @Override
    public List<byte[]> readBatch(List<byte[]> keys) {
        ArrayList<byte[]> values = new ArrayList<>();
        values.ensureCapacity(keys.size());
        for(int i=0;i<keys.size();i++){
            values.add(bPlusTree.find(new Record(keys.get(i), null)).getValue());
        }
        return values;
    }

    @Override
    public List<byte[]> rangeQuery(byte[] minKey, byte[] maxKey) {

        ArrayList<byte[]> values = new ArrayList<>();

        Iterator<Record> iterator = bPlusTree.iterator(new Record(minKey, null), new Record(maxKey, null));

        while (iterator.hasNext()) {
            values.add(iterator.next().getValue());
        }

        return values;
    }
}
