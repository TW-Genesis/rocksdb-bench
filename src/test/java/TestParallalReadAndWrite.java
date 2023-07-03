import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.apache.jena.dboe.transaction.Transactional;
import org.apache.jena.system.Txn;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestParallalReadAndWrite {

    @Test
    public void jenaBPTParallalWriteTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig), true);
        parallalReadAndWriteTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    public void RocksDBParallalWriteTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        parallalReadAndWriteTest(rocksdbKVStore, commonKVStoreConfig);
        TestUtils.dumpStats(rocksdbKVStore, "parallal-read-query.rocksdbstats");
        rocksdbKVStore.clean();
    }

    private void parallalReadAndWriteTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        int noOfPairsToWriteAndThenRead = 10000000;
        int batchSize = noOfPairsToWriteAndThenRead;
        
        TestUtils.measureThreadExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairsToWriteAndThenRead, commonKVStoreConfig.getKVSize()), batchSize);
        }, "batch write of batch size "+batchSize);

        TestUtils.measureWallClockExecutionTime(() -> {
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            List<Callable<Boolean>> tasks = new ArrayList<>();
            tasks.add(new WriteTask(kvStore, commonKVStoreConfig.getKVSize(), 10000001, 10000000));
            tasks.add(new ReadTask(kvStore, commonKVStoreConfig.getKVSize(),1, 10000000));
            // tasks.add(new ReadTask(kvStore, commonKVStoreConfig.getKVSize(),1, 10000000));

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
        }, "parallal read and write");
        
    }

    class WriteTask implements Callable<Boolean> {
        private final KVStore kvStore;
        private final int kvSize;
        private final int noOfPairs;
        private final int lowerLimit;
        private final int batchSize = 100000;
        
        public WriteTask(KVStore kvStore, int kvSize, int lowerLimit, int noOfPairs) {
            this.kvStore = kvStore;
            this.kvSize = kvSize;
            this.lowerLimit = lowerLimit;
            this.noOfPairs = noOfPairs;
        }

        @Override
        public Boolean call() {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, lowerLimit + noOfPairs - 1,kvSize), batchSize);
            return true;
        }
    }

    class ReadTask implements Callable<Boolean> {
        private final KVStore kvStore;
        private final int kvSize;
        private final int noOfPairs;
        private final int lowerLimit;
        private final Random random;     

        public ReadTask(KVStore kvStore, int kvSize, int lowerLimit, int noOfPairs) {
            this.kvStore = kvStore;
            this.kvSize = kvSize;
            this.random = new Random();
            this.lowerLimit = lowerLimit;
            this.noOfPairs = noOfPairs;
        }

        @Override
        public Boolean call() {
            for (int i = lowerLimit; i <= noOfPairs + lowerLimit - 1; i++) {
                byte[] fixedLengthKey = new byte[kvSize];
                TestUtils.copyStrToFixedLengthArr("k" +  ( random.nextInt(noOfPairs) + 1), fixedLengthKey);
                if(this.kvStore instanceof JenaBPTKVStore){
                    Transactional thing = ((JenaBPTKVStore)this.kvStore).geTransactional();
                    Txn.executeRead(thing, () -> {
                        kvStore.find(fixedLengthKey);
                    } );
                }else{
                    kvStore.find(fixedLengthKey);
                }
            }
            return true;
        }
    }
}
