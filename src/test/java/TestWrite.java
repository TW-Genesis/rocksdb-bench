import org.example.JenaBPTKVStore;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TestWrite {

    @BeforeAll
    public static void setup() throws RocksDBException {
        Logger logger = Logger.getLogger(TestWrite.class.getName());
        logger.setLevel(Level.INFO);

        KVStore[] kvStores = {new JenaBPTKVStore(), new RocksdbKVStore(new RocksdbKVStoreConfig1())};
        for (int i = 0; i < kvStores.length; i++) {
            logger.log(Level.INFO, "cleaning kv store: " + kvStores[i].getClass().getName());
            kvStores[i].clean();
        }
    }

    @Test
    public void jenaBPTWriteTest() {
        JenaBPTKVStore jenaBPTKVStore = new JenaBPTKVStore();
        writeWorkload(jenaBPTKVStore);
    }

    @Test
    void RocksDBWriteTest() throws RocksDBException {
        RocksdbKVStore rocksdbKVStore = new RocksdbKVStore(new RocksdbKVStoreConfig1());
        writeWorkload(rocksdbKVStore);
    }

    private void writeWorkload(KVStore kvStore) {
        int batchSize = 100000;
        TestUtils.measureThreadExecutionTime(() -> {
            kvStore.insertBatch(new TestUtils.KeyGenerator(), batchSize);
        }, "write with batch size of " + batchSize);
    }
}
