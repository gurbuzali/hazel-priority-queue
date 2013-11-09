package co.gurbuz.hazel.priorityqueue;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

/**
 * @ali 05/11/13
 */
public class MainTest {

    public static void main(String[] args) {

        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true);
        serviceConfig.setClassName(PriorityQueueService.class.getName());
        serviceConfig.setName(PriorityQueueService.SERVICE_NAME);

        final Config config = new Config();
        final ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(serviceConfig);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final IQueue q = instance.getDistributedObject(PriorityQueueService.SERVICE_NAME, "ali");
//        q.offer("veli");
//        q.offer("ali");
        System.err.println("item: " + q.poll());
        System.err.println("item: " + q.poll());

    }
}
