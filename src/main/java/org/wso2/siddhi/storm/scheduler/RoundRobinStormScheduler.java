package org.wso2.siddhi.storm.scheduler;

import backtype.storm.scheduler.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;


public class RoundRobinStormScheduler implements IScheduler {

    public static final String SIDDHI_TOPOLOGY_NAME = "LatencyMeasureTopology";
    private static Log log = LogFactory.getLog(RoundRobinStormScheduler.class);
    Map conf;
    Map<String, Map<String, SupervisorDetails>> componentToSupervisor = new HashMap<String, Map<String, SupervisorDetails>>();

    public void prepare(Map conf) {
        this.conf = conf;
    }

    private void indexSupervisors(Collection<SupervisorDetails> supervisorDetails) {
        for (SupervisorDetails supervisor : supervisorDetails) {
            Map meta = (Map) supervisor.getSchedulerMeta();
            String componentId = (String) meta.get("dedicated.to.component");

            if (componentId != null && !componentId.isEmpty()) {
                Map<String, SupervisorDetails> supervisorSet = componentToSupervisor.get(componentId);
                if (supervisorSet == null) {
                    supervisorSet = new HashMap<String, SupervisorDetails>();
                    componentToSupervisor.put(componentId, supervisorSet);
                }
                supervisorSet.put(supervisor.getId(), supervisor);
            }
        }
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        TopologyDetails siddhiTopology = topologies.getByName(SIDDHI_TOPOLOGY_NAME);

        if (siddhiTopology != null) {
            boolean isSchedulingNeeded = cluster.needsScheduling(siddhiTopology);

            if (!isSchedulingNeeded) {
                log.info(SIDDHI_TOPOLOGY_NAME + " already scheduled!");
            } else {
                log.info("Scheduling " + SIDDHI_TOPOLOGY_NAME);

                indexSupervisors(cluster.getSupervisors().values());

                Map<String, List<ExecutorDetails>> componentToExecutors =
                        cluster.getNeedsSchedulingComponentToExecutors(siddhiTopology);

                for (Map.Entry<String, List<ExecutorDetails>> entry : componentToExecutors.entrySet()) {
                    String componentName = entry.getKey();
                    List<ExecutorDetails> executors = entry.getValue();
                    if (componentToSupervisor.get(componentName) != null) {
                        Collection<SupervisorDetails> dedicatedSupervisorSet = componentToSupervisor.get(componentName).values();
                        if (!dedicatedSupervisorSet.isEmpty()) {
                            List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
                            List<Queue<WorkerSlot>> supervisorAvailableSlots = new ArrayList<Queue<WorkerSlot>>();
                            List<String> supervisorHostList = new ArrayList<String>();
                            for (SupervisorDetails supervisor : dedicatedSupervisorSet) {
                                Queue<WorkerSlot> slotQ = new LinkedBlockingQueue<WorkerSlot>();
                                slotQ.addAll(cluster.getAvailableSlots(supervisor));
                                supervisorAvailableSlots.add(slotQ);
                                supervisorHostList.add(supervisor.getHost());
                            }

                            int supervisorIndex = -1;
                            int supervisorCount = dedicatedSupervisorSet.size();
                            while (!supervisorAvailableSlots.isEmpty()) {
                                supervisorIndex = (supervisorIndex + 1) % supervisorCount;
                                Queue slotQ = supervisorAvailableSlots.get(supervisorIndex);
                                if (slotQ.isEmpty()) {
                                    supervisorAvailableSlots.remove(supervisorIndex);
                                    supervisorCount--;
                                    continue;
                                }
                                availableSlots.add((WorkerSlot) slotQ.poll());
                            }

                            if (log.isDebugEnabled()) {
                                log.debug("Scheduling " + componentName + " to supervisors " + Arrays.toString(supervisorHostList.toArray(new String[supervisorHostList.size()])));
                                log.debug("Available slots: ");
                                for (WorkerSlot availableSlot : availableSlots) {
                                    log.debug("Node ID: " + availableSlot.getNodeId());
                                }
                            }


                            if (availableSlots.isEmpty()) {
                                throw new RuntimeException("No free slots available for scheduling in dedicated supervisors @ " + Arrays.toString(supervisorHostList.toArray(new String[supervisorHostList.size()])));
                            }

                            int availableSlotCount = availableSlots.size();
                            int executorCount = executors.size();

                            List<List<ExecutorDetails>> executorsForSlots = new ArrayList<List<ExecutorDetails>>();
                            for (int i = 0; i < availableSlotCount; i++) {
                                executorsForSlots.add(new ArrayList<ExecutorDetails>());
                            }

                            for (int i = 0; i < executorCount; i++) {
                                int slotToAllocate = i % availableSlotCount;
                                ExecutorDetails executor = executors.get(i);
                                executorsForSlots.get(slotToAllocate).add(executor);
                            }

                            for (int i = 0; i < availableSlotCount; i++) {
                                if (!executorsForSlots.get(i).isEmpty()) {
                                    cluster.assign(availableSlots.get(i), siddhiTopology.getId(), executorsForSlots.get(i));
                                }
                            }
                        } else {
                            log.info("No dedicated supervisor for " + componentName);
                        }
                    }
                }

            }
        }
        new EvenScheduler().schedule(topologies, cluster);
    }
}


