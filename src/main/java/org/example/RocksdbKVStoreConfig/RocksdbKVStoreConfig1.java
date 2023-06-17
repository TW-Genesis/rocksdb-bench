package org.example.RocksdbKVStoreConfig;

import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.rocksdb.Options;

public class RocksdbKVStoreConfig1 implements RocksdbKVStoreConfig{
    private final CommonKVStoreConfig commonKVStoreConfig;

    public RocksdbKVStoreConfig1(CommonKVStoreConfig commonKVStoreConfig) {
        this.commonKVStoreConfig = commonKVStoreConfig;
    }

    @Override
    public Options getOption() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        // options.setIncreaseParallelism(1);
        return options;
    }

    @Override
    public int getKVSize() {
        return commonKVStoreConfig.getKVSize();
    }
}
