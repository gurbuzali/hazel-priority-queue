package co.gurbuz.hazel.priorityqueue;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;

import java.util.Random;

/**
 * @ali 05/11/13
 */
public class MainTest {

    static {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");

        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);

    }

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
        q.offer("veli");
        q.offer("ali");

        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);

        final IQueue q2 = instance2.getDistributedObject(PriorityQueueService.SERVICE_NAME, "ali");
        System.err.println("item: " + q2.poll());
        System.err.println("item: " + q2.poll());

        instance.shutdown();

    }
}
