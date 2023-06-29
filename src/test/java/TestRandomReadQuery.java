import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.Random;

public class TestRandomReadQuery {
    private final int noOfPairs = 10000000;

    @Test
    public void jenaBPTRandomReadTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig));
        randomReadTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBRandomReadTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        randomReadTest(rocksdbKVStore, commonKVStoreConfig);
        TestUtils.dumpStats(rocksdbKVStore, "random-read-query.rocksdbstats");
        rocksdbKVStore.clean();
    }

    private void randomReadTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        int batchSize = noOfPairs;
        TestUtils.measureExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs, commonKVStoreConfig.getKVSize()), batchSize);
        }, "batch write of batch size "+batchSize);
        Random random = new Random();

        TestUtils.measureExecutionTime(() -> {
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" +  ( random.nextInt(noOfPairs) + 1), fixedLengthKey);
                kvStore.find(fixedLengthKey);
            }
        }, "random search");
    }
}
