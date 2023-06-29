import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestSequentialReadQuery {
    private final int noOfPairs = 10000000;

    @Test
    public void jenaBPSequentialReadTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig));
        sequentialReadTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBSequentialReadTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        sequentialReadTest(rocksdbKVStore, commonKVStoreConfig);
        TestUtils.dumpStats(rocksdbKVStore, "sequential-read-query.rocksdbstats");
        rocksdbKVStore.clean();
    }

    private void sequentialReadTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        int batchSize = noOfPairs;
        TestUtils.measureExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs, commonKVStoreConfig.getKVSize()), batchSize);
        }, "batch write of batch size "+batchSize);

        TestUtils.measureExecutionTime(() -> {
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" + i, fixedLengthKey);
                kvStore.find(fixedLengthKey);
            }
        }, "random search");
    }
}
