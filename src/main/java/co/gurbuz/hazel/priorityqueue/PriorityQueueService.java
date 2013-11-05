package co.gurbuz.hazel.priorityqueue;

import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.QueueService;
import com.hazelcast.queue.proxy.QueueProxyImpl;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 05/11/13
 */
public class PriorityQueueService extends QueueService {

    public static final String SERVICE_NAME = "grbz:priorityQueueService";

    private final ConcurrentMap<String, PriorityQueueContainer> containerMap = new ConcurrentHashMap<String, PriorityQueueContainer>();

    NodeEngine nodeEngine;

    public PriorityQueueService(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.nodeEngine = nodeEngine;
    }

    public QueueContainer getOrCreateContainer(final String name, boolean fromBackup) throws Exception {
        PriorityQueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new PriorityQueueContainer(name, nodeEngine.getConfig().getQueueConfig(name), nodeEngine, this);

            PriorityQueueContainer existing = containerMap.putIfAbsent(name, container);
            if (existing != null) {
                container = existing;
            } else {
                container.init(fromBackup);
            }
        }
        container.cancelEvictionIfExists();
        return container;
    }

    public QueueProxyImpl createDistributedObject(String objectId) {
        return new PriorityQueueProxyImpl(objectId, nodeEngine, this);
    }

}
