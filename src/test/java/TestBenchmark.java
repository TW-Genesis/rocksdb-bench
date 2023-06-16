import org.junit.jupiter.api.*;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TestBenchmark {
    private final int kv_size = 10;
    private final String db_path = "/tmp/rocksdb-java";

    private RocksDB db;
    private Options options;
    @AfterEach
    void tearDown() throws RocksDBException {
        db.close();
        RocksDB.destroyDB(db_path, options);
    }

    @BeforeEach
    void setUp() throws RocksDBException {
        RocksDB.loadLibrary();
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        // options.setIncreaseParallelism(1);
        this.db = RocksDB.open(options, db_path);
    }

    @Test
    public void insertMultipleKeys() throws RocksDBException {
//        String BPT_dir = "/tmp/jena-bpt";
//        BPlusTree bPlusTree = BPlusTreeFactory.createBPTree(ComponentId.allocLocal(), new FileSet(BPT_dir, "bptree-java"), new RecordFactory(kv_size, kv_size));
//        bPlusTree.nonTransactional();

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        long cpuTimeStampBefore = threadMXBean.getCurrentThreadCpuTime();
        long wallClockStampBefore = System.nanoTime();

        int noOfPairs = 1000000;
        for (int i = 1; i <= noOfPairs; i++) {
            byte[] fixedLengthKey = new byte[kv_size];
            byte[] fixedLengthValue = new byte[kv_size];
            copyStrToFixedLengthArr("k" + Integer.toString(i), fixedLengthKey);
            copyStrToFixedLengthArr("v" + Integer.toString(i), fixedLengthValue);

            db.put(fixedLengthKey, fixedLengthValue);
//            bPlusTree.insert(new Record(fixedLengthKey, fixedLengthValue));
        }
        long cpuTimeStampAfter = threadMXBean.getCurrentThreadCpuTime();
        long wallClockStampAfter = System.nanoTime();

        System.out.println("insertion:");
        System.out.println("cpuTimeSpent            = " + (double) (cpuTimeStampAfter - cpuTimeStampBefore) / 1_000_000_000 + "s");
        System.out.println("wallClockTimeSpent      = " + (double) (wallClockStampAfter - wallClockStampBefore) / 1_000_000_000 + "s");

        cpuTimeStampBefore = threadMXBean.getCurrentThreadCpuTime();
        wallClockStampBefore = System.nanoTime();
        for (int i = 1; i <= noOfPairs; i++) {
            byte[] fixedLengthKey = new byte[kv_size];
            copyStrToFixedLengthArr("k" + Integer.toString(i), fixedLengthKey);
//            bPlusTree.find(new Record(fixedLengthKey, null));
            db.get(fixedLengthKey);
        }

        cpuTimeStampAfter = threadMXBean.getCurrentThreadCpuTime();
        wallClockStampAfter = System.nanoTime();

        System.out.println("search:");
        System.out.println("cpuTimeSpent in seconds            = " + (double) (cpuTimeStampAfter - cpuTimeStampBefore) / 1_000_000_000 + "s");
        System.out.println("wallClockTimeSpent in seconds      = " + (double) (wallClockStampAfter - wallClockStampBefore) / 1_000_000_000 + "s");

        byte[] fixedLengthKey = new byte[kv_size];
        byte[] fixedLengthValue = new byte[kv_size];
        copyStrToFixedLengthArr("k" + Integer.toString(1), fixedLengthKey);
        copyStrToFixedLengthArr("v" + Integer.toString(1), fixedLengthValue);

        Assertions.assertArrayEquals(db.get(fixedLengthKey), fixedLengthValue);
    }

    private static void copyStrToFixedLengthArr(String str, byte[] fixedLenArr) {
        for (int i = 0; i < fixedLenArr.length; i++) {
            fixedLenArr[i] = 0;
        }
        byte[] strInBytesForm = str.getBytes();
        int i = fixedLenArr.length - strInBytesForm.length;
        int j = 0;
        while (j < strInBytesForm.length) {
            fixedLenArr[i] = strInBytesForm[j];
            j++;
            i++;
        }
    }

}


