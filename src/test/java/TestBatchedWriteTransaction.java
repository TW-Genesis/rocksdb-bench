import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVPair;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.Iterator;

public class TestBatchedWriteTransaction {
    private final int noOfPairs = 10000000;

    @Test
    public void jenaBPTStressTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig));
        batchedWriteTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBStressTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        batchedWriteTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    private void batchedWriteTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        TestUtils.measureExecutionTime(() -> {
            kvStore.insertBatch(new IncrementalKVGenerator(1, noOfPairs+1, commonKVStoreConfig.getKVSize()));
        }, "insertion");

        TestUtils.measureExecutionTime(() -> {
            for (int i = 1; i <= noOfPairs; i++) {
                byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
                TestUtils.copyStrToFixedLengthArr("k" + i, fixedLengthKey);
                kvStore.find(fixedLengthKey);
            }
        }, "search");
    }

    public static class IncrementalKVGenerator implements Iterator<KVPair> {
        private final int upperLimit;
        private final int kvSize;
        private int currentIndex;

        public IncrementalKVGenerator(int lowerLimit, int upperLimit, int kvSize) {
            this.upperLimit = upperLimit;
            this.currentIndex = lowerLimit;
            this.kvSize = kvSize;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < upperLimit;
        }

        @Override
        public KVPair next() {
            byte[] fixedLengthKey = new byte[kvSize];
            byte[] fixedLengthValue = new byte[kvSize];
            TestUtils.copyStrToFixedLengthArr("k" + currentIndex, fixedLengthKey);
            TestUtils.copyStrToFixedLengthArr("v" + currentIndex, fixedLengthValue);
            currentIndex++;
            return new KVPair(fixedLengthKey, fixedLengthValue);
        }
    }

}
