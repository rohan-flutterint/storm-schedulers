package org.wso2.siddhi.storm.scheduler;

import backtype.storm.scheduler.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Created by sajith on 5/13/14.
 */
public class AttachComponentsScheduler implements IScheduler {
    private class SupervisorAllocations{
        private SupervisorDetails supervisor;
        private Map<String, Integer> allocatedComponents = new HashMap<String, Integer>();

        public SupervisorAllocations(SupervisorDetails supervisor){
            this.supervisor = supervisor;
        }

        public void addAllocation(String componentId, int count){
            allocatedComponents.put(componentId, count);
        }

        public Map<String, Integer> getAllocations(){
            return this.allocatedComponents;
        }

        public SupervisorDetails getSupervisorDetails(){return  this.supervisor;}
    }

    private static final String COMPONENT_DELIMITER = ",";
    private static final String VALUE_DELIMITER = "=";
    public static final String SIDDHI_TOPOLOGY_NAME = "LatencyMeasureTopology";
    private static Log log = LogFactory.getLog(AttachComponentsScheduler.class);
    Map<Integer, SupervisorAllocations> supervisorAllocationsMap = new HashMap<Integer, SupervisorAllocations>();
    Map conf;

    public void prepare(Map conf) {
        this.conf = conf;
    }

    // Decoding elements in CompID1=1,CompID2=*,CompID3=2 format
    private void indexAllocations(Collection<SupervisorDetails> supervisorDetails){
        for (SupervisorDetails supervisor : supervisorDetails) {
            Map meta = (Map) supervisor.getSchedulerMeta();
            String allocationConfig = (String) meta.get("component.allocation");

            if (allocationConfig != null && !allocationConfig.isEmpty()){
                int allocationOrdering = (meta.get("allocation.priority") == null) ? Integer.MAX_VALUE :
                        Integer.parseInt((String) meta.get("allocation.priority"));

                String[] allocations = allocationConfig.split(COMPONENT_DELIMITER);
                SupervisorAllocations supervisorAllocations = new SupervisorAllocations(supervisor);

                for (int i = 0; i < allocations.length; i++){
                    String allocation = allocations[i].trim();
                    String[] values = allocation.split(VALUE_DELIMITER);
                    supervisorAllocations.addAllocation(values[0].trim(), Integer.parseInt(values[1].trim()));
                    supervisorAllocationsMap.put(allocationOrdering, supervisorAllocations);
                    log.info(Integer.parseInt(values[1].trim()) + " instances of '" + values[0].trim() + "' configured to be assigned for " + supervisor.getHost());
                    // Putting into a hash map to enforce allocation ordering by it's automatic sorting
                }
            }
        }
    }

    private void freeAllSlots(SupervisorDetails supervisor, Cluster cluster){
        for (Integer port : cluster.getUsedPorts(supervisor)) {
            cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
        }
    }

    /**
     *
     * @param supervisorAllocations - Allocation details of the supervisor which contains the component names and instance count to be run on the supervisor
     * @param components - All the components that are available to be scheduled in the topology
     * @return Retrieve all executors that should be allocated to a supervisor by looking at allocation details
     */
    private List<ExecutorDetails> collectAllExecutorsForSuperVisor(SupervisorAllocations supervisorAllocations, Map<String, List<ExecutorDetails>> components){
        List<ExecutorDetails> executorsForSupervisor = new ArrayList<ExecutorDetails>();

        for (Map.Entry<String, Integer> allocationEntry : supervisorAllocations.getAllocations().entrySet()){
            String componentName = allocationEntry.getKey();
            int instanceCount = allocationEntry.getValue();
            List<ExecutorDetails> allExecutors = components.get(componentName);

            if (allExecutors == null){
                log.warn("Failed to find executors for component " + componentName);
                return executorsForSupervisor;
            }
            int allocatedInstanceCount = 0;

            for (;(allocatedInstanceCount < instanceCount)  && (!allExecutors.isEmpty()); allocatedInstanceCount++){
                    ExecutorDetails executor = allExecutors.get(0);
                    allExecutors.remove(0);
                    executorsForSupervisor.add(executor);
            }
            log.info(allocatedInstanceCount + " out of " + instanceCount + " instances of '" + componentName + "' allocated to be scheduled at " + supervisorAllocations.getSupervisorDetails().getHost());
        }

        return executorsForSupervisor;
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        log.info("Scheduling by Siddhi Storm Scheduler");
        TopologyDetails siddhiTopology = topologies.getByName(SIDDHI_TOPOLOGY_NAME);

        if (siddhiTopology != null){
            boolean isSchedulingNeeded = cluster.needsScheduling(siddhiTopology);

            if (!isSchedulingNeeded){
                log.info(SIDDHI_TOPOLOGY_NAME + " already scheduled!");
            }else{
                log.info("Scheduling "+ SIDDHI_TOPOLOGY_NAME);
                indexAllocations(cluster.getSupervisors().values());
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(siddhiTopology);

                if (componentToExecutors.isEmpty()){
                    return;
                }

                for (Map.Entry<Integer, SupervisorAllocations> entry : supervisorAllocationsMap.entrySet()){
                    SupervisorAllocations supervisorAllocations = entry.getValue();
                    List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisorAllocations.getSupervisorDetails());

                    if (availableSlots.isEmpty()){
                        freeAllSlots(supervisorAllocations.getSupervisorDetails(), cluster);
                        //throw new RuntimeException("No free slots available for scheduling in supervisor @ " + supervisorAllocations.getSupervisorDetails().getHost());
                    }

                    List<ExecutorDetails> executorsForSupervisor = collectAllExecutorsForSuperVisor(supervisorAllocations, componentToExecutors);
                    int totalExecutorCountForSupervisor = executorsForSupervisor.size();
                    int availableSlotCount = availableSlots.size();

                    int executorsPerSlot = (availableSlotCount > totalExecutorCountForSupervisor) ? 1 :
                            Math.round(totalExecutorCountForSupervisor/(float)availableSlotCount);

                    log.info("Scheduling " + totalExecutorCountForSupervisor + " executors across " + availableSlotCount + " slots of " +supervisorAllocations.getSupervisorDetails().getHost());

                    for (int i = 0; i < availableSlotCount; i++){
                        List<ExecutorDetails> executorsForSlot = new ArrayList<ExecutorDetails>();

                        for  (int j = 0; (j < executorsPerSlot) && (!executorsForSupervisor.isEmpty()); j++){
                            ExecutorDetails executorDetails = executorsForSupervisor.get(0);
                            executorsForSupervisor.remove(0);
                            executorsForSlot.add(executorDetails);
                        }

                        if (!executorsForSlot.isEmpty()){
                            //log.info("Scheduling " + executorsForSlot.size() + " executors for slot " + availableSlots.get(i).getPort() + " @ " + supervisorAllocations.getSupervisorDetails().getHost());
                            log.info("Port :" + availableSlots.get(i).getPort());
                            for (ExecutorDetails detials : executorsForSlot){
                                log.info("- " + detials.toString() + detials.getStartTask());
                            }
                            cluster.assign(availableSlots.get(i), siddhiTopology.getId(), executorsForSlot);
                        }
                    }

                    /*
                    if (!executorsForSupervisor.isEmpty()){
                        log.info("Sajith ++++++++++++++++++++++++++++++++++++++");
                        cluster.assign(availableSlots.get(0), siddhiTopology.getId(), executorsForSupervisor);
                    }*/
                }
            }
        }
        //new EvenScheduler().schedule(topologies, cluster);
    }
}
