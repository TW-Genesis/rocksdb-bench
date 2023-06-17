import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestStress {
    private final int noOfPairs = 1000000;

    @Test
    public void jenaBPTStressTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig));
        stressTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBStressTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        stressTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    private void stressTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        TestUtils.measureExecutionTime(() -> {
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                byte[] fixedLengthValue = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" + i, fixedLengthKey);
                TestUtils.copyStrToFixedLengthArr("v" + i, fixedLengthValue);
                kvStore.insert(fixedLengthKey, fixedLengthValue);
            }
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
