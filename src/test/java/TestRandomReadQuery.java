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
        KVStore kvStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        randomReadTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    private void randomReadTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        TestUtils.measureExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs + 1, commonKVStoreConfig.getKVSize()));
        }, "insertion");
        Random random = new Random();

        TestUtils.measureExecutionTime(() -> {
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" + (random.nextInt(noOfPairs) + 1), fixedLengthKey);
                kvStore.find(fixedLengthKey);
            }
        }, "random search");
    }
}
