import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.List;

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
        List<Integer> batchSizes = List.of(1000, 10000, 100000, 1000000, noOfPairs);
        for(int batchSize: batchSizes) {
            TestUtils.measureExecutionTime(() -> {
                kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs, commonKVStoreConfig.getKVSize()), batchSize);
            }, "batch write of batch size "+batchSize);
        }
    }

}
