import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;

public class TestBatchedReadQuery {
    private final int noOfPairs = 10000000;
    @Test
    public void jenaBPTBatchReadTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig), false);
        batchedReadTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBBatchReadTest() throws RocksDBException, IOException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        batchedReadTest(rocksdbKVStore, commonKVStoreConfig);
        TestUtils.dumpStats(rocksdbKVStore, "batched-reads.rocksdbstats");
        rocksdbKVStore.clean();
    }

    private void batchedReadTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        int batchSize = 100000000;
        TestUtils.measureThreadExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs, commonKVStoreConfig.getKVSize()), batchSize);
        }, "batch write of batch size "+ batchSize);

        TestUtils.measureThreadExecutionTime(() -> {
            ArrayList<byte[]> keys = new ArrayList<>();
            keys.ensureCapacity(noOfPairs);
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" + i, fixedLengthKey);
                keys.add(fixedLengthKey);
            }
            kvStore.readBatch(keys);
        }, "batch read");
    }

}
