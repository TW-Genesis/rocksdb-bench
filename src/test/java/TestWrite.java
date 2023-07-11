import org.example.JenaBPTKVStore;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestWrite {

    @Test
    public void jenaBPTWriteTest() {
        JenaBPTKVStore jenaBPTKVStore = new JenaBPTKVStore();
        writeWorkload(jenaBPTKVStore);
        // jenaBPTKVStore.clean();
    }

    @Test
    void RocksDBWriteTest() throws RocksDBException {
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1());
        writeWorkload(rocksdbKVStore);
        // rocksdbKVStore.clean();
    }

    private void writeWorkload(KVStore kvStore) {
        WorkloadConfiguration workloadConfiguration = WorkloadConfiguration.getWorkloadConfiguration();
        int batchSize = 100000;
        TestUtils.measureThreadExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.KeyGenerator(workloadConfiguration), batchSize);
            // kvStore.flush();
        }, "write with batch size of "+ batchSize);
    }
}
