import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.JenaBPTKVStore;
import org.example.KVPair;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestParallalWrite {

    @Test
    public void jenaBPTParallalWriteTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig), true);
        parallalWriteTest(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    public void RocksDBParallalWriteTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        parallalWriteTest(rocksdbKVStore, commonKVStoreConfig);
        rocksdbKVStore.clean();
    }

    private void parallalWriteTest(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        List<Iterator<KVPair>> batches = new ArrayList<>();
        batches.add(new TestUtils.IncrementalKVGenerator(1, 10000000, commonKVStoreConfig.getKVSize()));
        batches.add(new TestUtils.IncrementalKVGenerator(10000001, 20000000, commonKVStoreConfig.getKVSize()));
        batches.add(new TestUtils.IncrementalKVGenerator(20000001, 30000000, commonKVStoreConfig.getKVSize()));
        batches.add(new TestUtils.IncrementalKVGenerator(30000001, 40000000, commonKVStoreConfig.getKVSize()));
        TestUtils.measureWallClockExecutionTime(() -> {
            kvStore.insertConcurrently(batches, 1000000);
        }, "write parallal batches");
    }
}
