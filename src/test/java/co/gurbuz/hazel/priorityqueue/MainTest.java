package co.gurbuz.hazel.priorityqueue;

import co.gurbuz.hazel.priorityqueue.client.PriorityPortableHook;
import co.gurbuz.hazel.priorityqueue.client.ProxyFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;

import java.util.List;
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

        SerializationConfig memberSerializationConfig = config.getSerializationConfig();
        PriorityPortableHook hook = new PriorityPortableHook();
        memberSerializationConfig.addPortableFactory(PriorityPortableHook.F_ID, hook.createFactory());

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        IQueue memberQ = instance.getDistributedObject(PriorityQueueService.SERVICE_NAME, "foo");

        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setClassName(ProxyFactory.class.getName());
        proxyFactoryConfig.setService(PriorityQueueService.SERVICE_NAME);
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);
        SerializationConfig clientSerializationConfig = clientConfig.getSerializationConfig();
        clientSerializationConfig.addPortableFactory(PriorityPortableHook.F_ID, hook.createFactory());

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IQueue clientQ = client.getDistributedObject(PriorityQueueService.SERVICE_NAME, "foo");
        clientQ.offer("veli");
        clientQ.offer("ali");

        Object ali = memberQ.poll();
        Object veli = memberQ.poll();
        System.err.println("ali: " + ali);
        System.err.println("veli: " + veli);
    }
}
