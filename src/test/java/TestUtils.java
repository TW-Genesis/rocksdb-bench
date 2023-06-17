import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TestUtils {
    public static void copyStrToFixedLengthArr(String str, byte[] fixedLenArr) {
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

    public static void measureExecutionTime(Runnable function, String operation) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long cpuTimeStampBefore = threadMXBean.getCurrentThreadCpuTime();

        function.run();

        long cpuTimeStampAfter = threadMXBean.getCurrentThreadCpuTime();
        System.out.println(operation + ":");
        System.out.println("cpuTimeSpent            = " + (double) (cpuTimeStampAfter - cpuTimeStampBefore) / 1_000_000_000 + "s");
    }

}
