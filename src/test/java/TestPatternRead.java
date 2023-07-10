import org.example.JenaBPTKVStore;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestPatternRead {
    @Test
    public void jenaBPPatternReadTest() {
        JenaBPTKVStore jenaBPTKVStore = new JenaBPTKVStore(false);
        patternReadTest(jenaBPTKVStore);
        // jenaBPTKVStore.clean();
    }

    @Test
    void RocksDBPatternReadTest() throws RocksDBException {
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1());
        patternReadTest(rocksdbKVStore);
        // rocksdbKVStore.clean();
    }

    private void patternReadTest(KVStore kvStore) {
        TestUtils.measureThreadExecutionTime(() -> {
            byte[] S = TestUtils.convertToByteArray(0);
            byte[] FromP = TestUtils.convertToByteArray(0);
            byte[] ToP = TestUtils.convertToByteArray(1);
            byte[] O = TestUtils.convertToByteArray(0);
            byte[] FromKey = TestUtils.getKey(S, FromP, O);
            byte[] ToKey = TestUtils.getKey(S, ToP, O);
            kvStore.rangeQuery(FromKey, ToKey);
        }, "range search");
    }
}
