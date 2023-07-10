import org.example.KVPair;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.Iterator;


public class TestUtils {
    public static void measureThreadExecutionTime(Runnable function, String operation) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long cpuTimeStampBefore = threadMXBean.getCurrentThreadCpuTime();

        function.run();

        long cpuTimeStampAfter = threadMXBean.getCurrentThreadCpuTime();
        System.out.println(operation + ":");
        System.out.println("cpuTimeSpent            = " + (double) (cpuTimeStampAfter - cpuTimeStampBefore) / 1_000_000_000 + "s");
    }

    public static void measureWallClockExecutionTime(Runnable function, String operation) {
        long cpuTimeStampBefore = System.nanoTime();

        function.run();

        long cpuTimeStampAfter = System.nanoTime();
        System.out.println(operation + ":");
        System.out.println("wallClockTimeSpent            = " + (double) (cpuTimeStampAfter - cpuTimeStampBefore) / 1_000_000_000 + "s");
    }

    public static byte[] convertToByteArray(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static byte[] getKey(byte[] S, byte[] P, byte[] O){
        byte[] Key = new byte[S.length + P.length + O.length];
        System.arraycopy(S, 0, Key, 0, S.length);
        System.arraycopy(P, 0, Key, S.length, P.length);
        System.arraycopy(O, 0, Key, S.length + P.length, O.length);
        return Key;
    }

    public static class IncrementalKeyGenerator implements Iterator<KVPair> {
        private final int noOfObjects;
        private int noOfObjectsLeft;
        private final byte[] prefixS ;
        private final byte[] prefixP ;

        public IncrementalKeyGenerator(long prefixS, long prefixP, int noOfObjects) {
            this.noOfObjects = noOfObjects;
            this.noOfObjectsLeft = noOfObjects;
            this.prefixS = convertToByteArray(prefixS);
            this.prefixP = convertToByteArray(prefixP);
        }

        @Override
        public boolean hasNext() {
            return noOfObjectsLeft > 0;
        }

        @Override
        public KVPair next() {
            if(!hasNext()){
                throw new RuntimeException("no keys left");
            }
            byte[] object = convertToByteArray(noOfObjects-noOfObjectsLeft+1);
            byte[] Key = getKey(prefixS, prefixP, object);
            noOfObjectsLeft--;
            //fixedLengthKey[0] is Most significant bit
            //fixedLengthKey[23] is Least significant bit
            //fixedLengthKey[] = [0][1][2][3]...[23]
            //fixedLengthKey = [s 8bytes] [p 8bytes] [o 8bytes]

            return new KVPair(Key, null);
        }
    }
}
