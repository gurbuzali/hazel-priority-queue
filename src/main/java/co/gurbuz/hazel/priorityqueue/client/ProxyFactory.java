package co.gurbuz.hazel.priorityqueue.client;

import co.gurbuz.hazel.priorityqueue.PriorityQueueService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;


public class ProxyFactory implements ClientProxyFactory {

    @Override
    public ClientProxy create(String s) {
        return new ClientPriorityQueueProxyImpl(PriorityQueueService.SERVICE_NAME, s);
    }
}
