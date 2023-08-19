package org.example.RocksdbKVStoreConfig;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

public class RocksdbKVStoreSingleSSTConfig implements RocksdbKVStoreConfig {

    @Override
    public Options getOption() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.NO_COMPRESSION);
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        options.setNumLevels(1);
        return options;
    }
}
