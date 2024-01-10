package org.example.RocksdbKVStoreConfig;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.Priority;

public class RocksdbKVStoreConfig3 implements RocksdbKVStoreConfig {

    @Override
    public Options getOption() {
        Options options = new Options();
        options.setCreateIfMissing(true);

        //compaction
        options.setLevelCompactionDynamicLevelBytes(true);
        options.setMaxBytesForLevelBase(800000000);
        options.setNumLevels(6);

        //compression
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);

        //memtable
        options.setWriteBufferSize(300000000);
        options.setMaxWriteBufferNumber(8);
        options.setMinWriteBufferNumberToMerge(2);

        //Prefix bloom filter
        BlockBasedTableConfig tableConfig;
        BloomFilter bloomFilterPolicy;
        tableConfig = new BlockBasedTableConfig();
        bloomFilterPolicy = new BloomFilter(10, false);
        tableConfig.setFilterPolicy(bloomFilterPolicy);
        tableConfig.setWholeKeyFiltering(false);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.useCappedPrefixExtractor(16);

        //Open files
        options.setMaxOpenFiles(2000);

        //memtable prefix
        options.setMemtablePrefixBloomSizeRatio(0.25);

        //BackGroundThreads
        Env env = options.getEnv();
        env.setBackgroundThreads(4, Priority.HIGH);
        env.setBackgroundThreads(4, Priority.LOW);
        return options;
    }

}