import org.example.JenaBPTKVStore;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestBatchWrite {
    private final int noOfPairs = 10000000;

    @Test
    public void jenaBPSequentialWriteTest() {
        JenaBPTKVStore jenaBPTKVStore = new JenaBPTKVStore(false);
        batchWriteTest(jenaBPTKVStore);
        // jenaBPTKVStore.clean();
    }

    @Test
    void RocksDBSequentialWriteTest() throws RocksDBException {
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1());
        batchWriteTest(rocksdbKVStore);
        // rocksdbKVStore.clean();
    }

    private void batchWriteTest(KVStore kvStore) {
        int batchSize = noOfPairs;
        TestUtils.measureThreadExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKeyGenerator(0, 0, noOfPairs), batchSize);
            // kvStore.flush();
        }, "batch write of batch size "+batchSize);
    }
}
