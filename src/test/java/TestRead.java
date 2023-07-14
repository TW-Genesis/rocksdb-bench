import java.util.Arrays;

import org.example.JenaBPTKVStore;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestRead {
    private final int numOfSPPairsToRead = 100;
    @Test
    public void jenaBPPatternReadTest() {
        JenaBPTKVStore jenaBPTKVStore = new JenaBPTKVStore();
        patternReadTest(jenaBPTKVStore);
    }

    @Test
    void RocksDBPatternReadTest() throws RocksDBException {
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1());
        patternReadTest(rocksdbKVStore);
    }

    private void patternReadTest(KVStore kvStore) {
        TestUtils.measureThreadExecutionTime(() -> {
            TestUtils.RandomSPPairGenerator randomSPPairGenerator = new TestUtils.RandomSPPairGenerator(WorkloadConfiguration.getWorkloadConfiguration(), numOfSPPairsToRead);
            while(randomSPPairGenerator.hasNext()){
                byte[] spPair = randomSPPairGenerator.next();
                byte[] O = TestUtils.convertToByteArray(0);
                byte[] FromKey = TestUtils.append(spPair, O);
                byte[] ToKey = TestUtils.append(getEdgeSPPair(spPair), O);
                kvStore.rangeQuery(FromKey, ToKey);
            }
        }, "search");
    }

    private byte[] getEdgeSPPair(byte[] spPair) {
        byte[] edgeSPPair = Arrays.copyOf(spPair, spPair.length);
        for(int i = edgeSPPair.length-1 ; i>=0; i--){
            edgeSPPair[i]++;
            if(edgeSPPair[i] != 0)
                break;
        }
        return edgeSPPair;
    }
}
