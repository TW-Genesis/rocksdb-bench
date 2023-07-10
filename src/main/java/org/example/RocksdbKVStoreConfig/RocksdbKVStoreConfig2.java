package org.example.RocksdbKVStoreConfig;

import org.rocksdb.Options;

public class RocksdbKVStoreConfig2 implements RocksdbKVStoreConfig{


    @Override
    public Options getOption() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setIncreaseParallelism(16);
        // options.setIncreaseParallelism(1);
        return options;
    }

    
}
