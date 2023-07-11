public class WorkloadConfiguration {
    public static final int numOfDistinctSPPairs = 100;
    public static final int variance = 5;
    public static final int mean = 10;
    private static WorkloadConfiguration workloadConfiguration = null;
    private WorkloadConfiguration(){
    }
    public static WorkloadConfiguration getWorkloadConfiguration(){
        if(workloadConfiguration == null){
            workloadConfiguration = new WorkloadConfiguration();
        }
        return workloadConfiguration;
    }
}