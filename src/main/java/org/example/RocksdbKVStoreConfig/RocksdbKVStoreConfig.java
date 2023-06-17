package org.example.RocksdbKVStoreConfig;

import org.example.CommonKVStoreConfig.CommonKVStoreConfig;
import org.rocksdb.Options;

public interface RocksdbKVStoreConfig extends CommonKVStoreConfig {
    Options getOption();
}
