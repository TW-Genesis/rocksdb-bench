import org.example.KVPair;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;


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

    public static byte[] append(byte[] arr1, byte[] arr2){
        byte[] finalArr = new byte[arr1.length + arr2.length];
        System.arraycopy(arr1, 0, finalArr, 0, arr1.length);
        System.arraycopy(arr2, 0, finalArr, arr1.length, arr2.length);
        return finalArr;
    }

    //Key[0] is Most significant byte
    //Key[23] is Least significant byte
    //Key[] = [0][1][2][3]...[23]
    //Key[] = [s 8bytes] [p 8bytes] [o 8bytes]

    public static class KeyGenerator implements Iterator<KVPair> {
        private final WorkloadConfiguration workloadConfiguration;
        private int noOfSPPairsLeft;
        private int noOfObjectsLeft;

        public KeyGenerator(WorkloadConfiguration workloadConfiguration) {
            this.noOfSPPairsLeft = workloadConfiguration.numOfDistinctSPPairs;
            this.noOfObjectsLeft = 0;
            this.workloadConfiguration = workloadConfiguration;
        }

        @Override
        public boolean hasNext() {
            return noOfObjectsLeft > 0 || noOfSPPairsLeft > 0;
        }

        @Override
        public KVPair next() {
            if(!hasNext()){
                throw new RuntimeException("no keys left");
            }  
            byte[] S,P,O;
            if(noOfObjectsLeft > 0){
                S = convertToByteArray(this.noOfSPPairsLeft+1);
                P = convertToByteArray(this.noOfSPPairsLeft+1);
                O = convertToByteArray(this.noOfObjectsLeft--);
            } else {
                S = convertToByteArray(this.noOfSPPairsLeft);
                P = convertToByteArray(this.noOfSPPairsLeft--);
                Random random = new Random();
                this.noOfObjectsLeft = random.nextInt(workloadConfiguration.variance) + 1 + workloadConfiguration.mean;
                O = convertToByteArray(this.noOfObjectsLeft--);
            }
            byte[] Key = append(append(S, P), O);
            return new KVPair(Key, null);
        }
    }

    public static class RandomSPPairGenerator implements Iterator<byte[]> {
        private final WorkloadConfiguration workloadConfiguration;
        private int noOfSPPairsLeft;

        public RandomSPPairGenerator(WorkloadConfiguration workloadConfiguration, int numOfPairsToGenerate) {
            this.noOfSPPairsLeft = numOfPairsToGenerate;
            this.workloadConfiguration = workloadConfiguration;
        }

        @Override
        public boolean hasNext() {
            return noOfSPPairsLeft > 0;
        }

        @Override
        public byte[] next() {
            if(!hasNext()){
                throw new RuntimeException("no prefixes left");
            }  
            Random random = new Random();
            byte[] S = convertToByteArray(random.nextInt(workloadConfiguration.numOfDistinctSPPairs) + 1);
            this.noOfSPPairsLeft--;
            return append(S, S);
        }
    }
}
