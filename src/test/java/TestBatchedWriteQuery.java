import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestBatchedWriteQuery {
    private final int noOfPairs = 10000000;

    @Test
    public void jenaBPTBatchWriteTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig));
        batchedWriteTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBBatchWriteTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        batchedWriteTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    private void batchedWriteTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        TestUtils.measureExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs+1, commonKVStoreConfig.getKVSize()));
        }, "insertion");

        TestUtils.measureExecutionTime(() -> {
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" + i, fixedLengthKey);
                kvStore.find(fixedLengthKey);
            }
        }, "search");
    }

}
