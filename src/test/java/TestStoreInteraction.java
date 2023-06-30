import org.example.CommonKVStoreConfig.CommonKVStoreConfig1;
import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.example.JenaBPTKVStore;
import org.example.JenaBPTKVStoreConfig.JenaBPTKVStoreConfig1;
import org.example.KVStore;
import org.example.RocksdbKVStore;
import org.example.RocksdbKVStoreConfig.RocksdbKVStoreConfig1;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

public class TestStoreInteraction {

    @Test
    public void jenaBPTInteractionTest() {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new JenaBPTKVStore(new JenaBPTKVStoreConfig1(commonKVStoreConfig), false);
        isInteractionValid(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    @Test
    void RocksDBInteractionTest() throws RocksDBException {
        CommonKVStoreConfig commonKVStoreConfig = new CommonKVStoreConfig1();
        KVStore kvStore = new RocksdbKVStore(new RocksdbKVStoreConfig1(commonKVStoreConfig));
        isInteractionValid(kvStore, commonKVStoreConfig);
        kvStore.clean();
    }

    private void isInteractionValid(KVStore kvStore, CommonKVStoreConfig commonKVStoreConfig) {
        byte[] fixedLengthKey = new byte[commonKVStoreConfig.getKVSize()];
        byte[] fixedLengthValue = new byte[commonKVStoreConfig.getKVSize()];
        TestUtils.copyStrToFixedLengthArr("k" + 1, fixedLengthKey);
        TestUtils.copyStrToFixedLengthArr("v" + 1, fixedLengthValue);
        kvStore.insert(fixedLengthKey, fixedLengthValue);
        Assertions.assertArrayEquals(fixedLengthValue, kvStore.find(fixedLengthKey));
    }
}
