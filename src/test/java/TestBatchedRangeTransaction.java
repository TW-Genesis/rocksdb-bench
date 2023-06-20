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

public class TestBatchedRangeTransaction {
    private final int noOfPairs = 10000000;

    @Test
    public void jenaBPTStressTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig));
        rangeQueryTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBStressTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        rangeQueryTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    private void rangeQueryTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        TestUtils.measureExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.IncrementalKVGenerator(1, noOfPairs+1, commonKVStoreConfig.getKVSize()));
        }, "insertion");

        TestUtils.measureExecutionTime(() -> {
            byte[] minKey = new byte[commonKVStoreConfig.getKVSize()];
            TestUtils.copyStrToFixedLengthArr("k" + 1, minKey);
            byte[] maxKey = new byte[commonKVStoreConfig.getKVSize()];
            TestUtils.copyStrToFixedLengthArr("k" + noOfPairs, maxKey);
            List<byte[]> values =  kvStore.rangeQuery(minKey, maxKey);
        }, "search");
    }

}
