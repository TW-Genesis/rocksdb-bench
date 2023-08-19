# rocksdb-bench

## Performance Scores of RocksDb and Apache Jena

```
Apply the tuning parameters provided by RocksDB in order to increase 
read performance of RocksDb. 
Compare the read, write and memory scores with Apache Jena B+ tree.
```

## Experiment Setup

```
Machine 1 - Intel core i7 - 9750H CPU @2.60 GHz x 12 (SSD)
Machine 2 - AMD Ryzen 7 5700G x86_64 (NVME)
```

## Workload configuration
```
Consider S(Subject), P(Perdicate) and O(Object) as input triples. Each of size
8 bytes. Here key is SPO of size 24 bytes. For each SP pair we have N number
of Object matches. 
```
![img_1.png](img.png)

## Tuning Parameters
- ReadAhead size
- Direct I/O 
- Data block Size 
- Disable Compression and Checksum
- Bloom Filter 
- Prefix Bloom filter 
- Memtable Bloom filter
- Asynchronous I/O  
- Compaction Styles
- Compression types
- writeBufferSize, maxWriteBufferNumber, minWriteBufferNumberToMerge 
- maxOpenFiles 
- memtablePrefixBloomSizeRatio 
- Block Cache 
- cacheIndexAndFilterBlocks
- Single SST file
- Parallelism

## Result
```
For workload type-4 by removing compression and checksum we were able
to increase read score of rocksDB ( 43.4521s for 100 million pairs)
which was (62.4660s for default configuration).Jena B+ tree uses more memory 
than RocksDB. Maybe as RockDB uses compression techniques it takes much less 
space than Jena. 

The read performance of RocksDb increased when all the sst files were 
compacted into single sst file using Universal compaction by LDB tool. 
```

## Benchmark scores
https://github.com/TW-Genesis/rocksdb-bench/blob/main/RocksDb-vs-Jena.csv
