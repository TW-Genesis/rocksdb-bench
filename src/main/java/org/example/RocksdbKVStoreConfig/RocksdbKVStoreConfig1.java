package org.example.RocksdbKVStoreConfig;

import org.rocksdb.Options;

public class RocksdbKVStoreConfig1 implements RocksdbKVStoreConfig{

    @Override
    public Options getOption() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        // options.setOptimizeFiltersForHits(false);
        // options.setIncreaseParallelism(1);
        return options;
    }

}
