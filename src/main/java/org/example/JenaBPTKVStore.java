package org.example;

import org.apache.jena.dboe.base.file.FileSet;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.dboe.trans.bplustree.BPlusTreeFactory;
import org.apache.jena.dboe.transaction.Transactional;
import org.apache.jena.dboe.transaction.TransactionalFactory;
import org.apache.jena.dboe.transaction.txn.ComponentId;
import org.apache.jena.dboe.transaction.txn.TransactionalComponent;
import org.apache.jena.system.Txn;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JenaBPTKVStore implements KVStore{
    private final String BPT_dir = "/home/e4r/test-database/jena-bpt";
    private final BPlusTree bPlusTree;
    private final Boolean isTransactional;
    private final Transactional thing;

    public JenaBPTKVStore(JenaBPTKVStoreConfig jenaBPTKVStoreConfig, boolean isTransactional) {
        this.bPlusTree = BPlusTreeFactory.createBPTree(ComponentId.allocLocal(), new FileSet(BPT_dir, "bptree-java"), new RecordFactory(jenaBPTKVStoreConfig.getKVSize(), jenaBPTKVStoreConfig.getKVSize()));
        this.isTransactional = isTransactional;
        if(!this.isTransactional) this.bPlusTree.nonTransactional();
        this.thing = transactional(bPlusTree);
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
        if(this.isTransactional){
            Transactional thing = transactional(bPlusTree);
            Txn.executeWrite(thing, () -> {
                bPlusTree.clear();
            } );
        }else{
            bPlusTree.clear();
        }
        bPlusTree.close();
    }

    @Override
    public void insertBatch(Iterator<KVPair> kvPairs, int batchSize) {
        insertBatch(kvPairs, batchSize, bPlusTree, isTransactional, this.thing);
    }

    @Override
    public void readBatch(List<byte[]> keys) {
        for (int i = 0; i < keys.size(); i++) {
            bPlusTree.find(new Record(keys.get(i), null)).getValue();
        }
    }

    @Override
    public void rangeQuery(byte[] minKey, byte[] maxKey) {
        Iterator<Record> iterator = bPlusTree.iterator(new Record(minKey, null), new Record(maxKey, null));

        while (iterator.hasNext()) {
            iterator.next().getValue();
        }
    }

    static Transactional transactional(TransactionalComponent ... components) {
        return transactional(Location.mem(), components);
    }

    static Transactional transactional(Location location, TransactionalComponent ... components) {
        return TransactionalFactory.createTransactional(location, components);
    }
    
    public static void insertBatch(Iterator<KVPair> kvPairs, int batchSize, BPlusTree bPlusTree, Boolean isTransactional, Transactional thing){
        if(isTransactional){
            Txn.executeWrite(thing, () -> {
                while (kvPairs.hasNext()) {
                    KVPair kvPair = kvPairs.next();
                    bPlusTree.insert(new Record(kvPair.key, kvPair.value));
                }
            } );
        }else{
            while (kvPairs.hasNext()) {
                KVPair kvPair = kvPairs.next();
                bPlusTree.insert(new Record(kvPair.key, kvPair.value));
            }
        }
    }

    @Override
    public void insertConcurrently(List<Iterator<KVPair>> kvPairsList, int batchSize) {
        if(!this.isTransactional)
            throw new RuntimeException("bplustree is not tranactional!");
        ExecutorService executorService = Executors.newFixedThreadPool(kvPairsList.size());
        List<WriteTask> tasks = new ArrayList<>();
        for(int i=0;i< kvPairsList.size();i++)
            tasks.add(new WriteTask(kvPairsList.get(i), batchSize, bPlusTree, this.thing));
        try {
            executorService.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
        try {
            if(executorService.awaitTermination(1000, TimeUnit.SECONDS)){
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class WriteTask implements Callable<Boolean> {
        private final Iterator<KVPair> kvPairs;
        private final BPlusTree bPlusTree;
        private final int batchSize;
        private final Transactional thing;

        public WriteTask(Iterator<KVPair> kvPairs, int batchSize, BPlusTree bPlusTree, Transactional thing) {
            this.kvPairs = kvPairs;
            this.bPlusTree = bPlusTree;
            this.batchSize = batchSize;
            this.thing = thing;
        }

        @Override
        public Boolean call() {
            insertBatch(kvPairs, batchSize, bPlusTree, true, thing);
            return true;
        }
    }
}
