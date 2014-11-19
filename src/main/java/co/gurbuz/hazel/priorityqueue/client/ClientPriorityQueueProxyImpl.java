package co.gurbuz.hazel.priorityqueue.client;

import com.hazelcast.client.proxy.ClientQueueProxy;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ClientPriorityQueueProxyImpl<E> extends ClientProxy implements IQueue<E> {

    final ClientQueueProxy<E> queueProxy;

    public ClientPriorityQueueProxyImpl(String serviceName, String objectName) {
        super(serviceName, objectName);
        queueProxy = new ClientQueueProxy<E>(serviceName, objectName);
    }

    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        return queueProxy.addItemListener(listener, includeValue);
    }

    @Override
    public boolean removeItemListener(String registrationId) {
        return queueProxy.removeItemListener(registrationId);
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        return queueProxy.getLocalQueueStats();
    }

    public boolean add(E e) {
        return queueProxy.add(e);
    }

    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    public void put(E e) throws InterruptedException {
        queueProxy.put(e);
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        Data data = getContext().getSerializationService().toData(e);
        PriorityOfferRequest request = new PriorityOfferRequest(getName(), unit.toMillis(timeout), data);
        final Boolean result = invoke(request);
        return result;
    }

    @Override
    public E take() throws InterruptedException {
        return queueProxy.take();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queueProxy.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return queueProxy.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        return queueProxy.remove(o);
    }

    @Override
    public boolean contains(Object o) {
        return queueProxy.contains(o);
    }

    public int drainTo(Collection<? super E> objects) {
        return queueProxy.drainTo(objects);
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        return queueProxy.drainTo(c, maxElements);
    }

    @Override
    public E remove() {
        return queueProxy.remove();
    }

    @Override
    public E poll() {
        return queueProxy.poll();
    }

    @Override
    public E element() {
        return queueProxy.element();
    }

    @Override
    public E peek() {
        return queueProxy.peek();
    }

    @Override
    public int size() {
        return queueProxy.size();
    }

    @Override
    public boolean isEmpty() {
        return queueProxy.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return queueProxy.iterator();
    }

    @Override
    public Object[] toArray() {
        return queueProxy.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return queueProxy.toArray(ts);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return queueProxy.containsAll(c);
    }

    public boolean addAll(Collection<? extends E> c) {
        return queueProxy.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return queueProxy.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return queueProxy.retainAll(c);
    }

    @Override
    public void clear() {
        queueProxy.clear();
    }

    @Override
    protected void onDestroy() {
        queueProxy.destroy();
    }

    private <T> T invoke(Object req){
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getPartitionKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
