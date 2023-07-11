public class WorkloadConfiguration {
    public static final int numOfDistinctSPPairs = 1000;
    public static final int variance = 1;
    public static final int spMatches = 10000;
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