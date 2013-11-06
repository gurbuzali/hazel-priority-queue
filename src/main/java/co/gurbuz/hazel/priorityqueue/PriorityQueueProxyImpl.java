package co.gurbuz.hazel.priorityqueue;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.*;
import com.hazelcast.queue.proxy.QueueIterator;
import com.hazelcast.queue.proxy.QueueProxyImpl;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @ali 05/11/13
 */
public class PriorityQueueProxyImpl<E> extends QueueProxyImpl<E> {

    final String name;
    final int partitionId;
    final QueueConfig config;

    public PriorityQueueProxyImpl(String name, NodeEngine nodeEngine, PriorityQueueService service) {
        super(name, service, nodeEngine);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
        this.config = nodeEngine.getConfig().getQueueConfig(name);
    }

    public boolean add(E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    public void put(E e) throws InterruptedException {
        offer(e, -1, TimeUnit.MILLISECONDS);
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        throwExceptionIfNull(e);
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(e);
        final OfferOperation operation = new OfferOperation(name, unit.toMillis(timeout), data);
        return (Boolean)invoke(operation);
    }

    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        final PollOperation operation = new PollOperation(name, unit.toMillis(timeout));
        return invoke(operation);
    }

    public int remainingCapacity() {
        return config.getMaxSize() - size();
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        RemoveOperation operation = new RemoveOperation(name, data);
        return (Boolean)invoke(operation);
    }

    public boolean contains(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        List<Data> dataList = new ArrayList<Data>(1);
        dataList.add(data);
        ContainsOperation operation = new ContainsOperation(name, dataList);
        return (Boolean)invoke(operation);
    }

    public int drainTo(Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    public int drainTo(Collection<? super E> objects, int maxElements) {
        final NodeEngine nodeEngine = getNodeEngine();
        if (this.equals(objects)) {
            throw new IllegalArgumentException("Can not drain to same Queue");
        }
        DrainOperation operation = new DrainOperation(name, maxElements);
        SerializableCollection collectionContainer = invoke(operation);
        Collection<Data> dataList = collectionContainer.getCollection();
        for (Data data : dataList) {
            E e = nodeEngine.toObject(data);
            objects.add(e);
        }
        return dataList.size();
    }

    public E remove() {
        final E res = poll();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public E poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public E element() {
        final E res = peek();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public E peek() {
        PeekOperation operation = new PeekOperation(name);
        return invoke(operation);
    }

    public int size() {
        SizeOperation operation = new SizeOperation(name);
        return (Integer)invoke(operation);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Iterator<E> iterator() {
        final NodeEngine nodeEngine = getNodeEngine();
        return new QueueIterator<E>(listInternal().iterator(), nodeEngine.getSerializationService(), false);
    }

    public Object[] toArray() {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> list = listInternal();
        int size = list.size();
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = nodeEngine.toObject(list.get(i));
        }
        return array;
    }

    public <T> T[] toArray(T[] ts) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> list = listInternal();
        int size = list.size();
        if (ts.length < size) {
            ts = (T[]) java.lang.reflect.Array.newInstance(ts.getClass().getComponentType(), size);
        }
        for (int i = 0; i < size; i++) {
            ts[i] = nodeEngine.toObject(list.get(i));
        }
        return ts;
    }

    public boolean containsAll(Collection<?> c) {
        ContainsOperation operation = new ContainsOperation(name, getDataList(c));
        return (Boolean)invoke(operation);
    }

    public boolean addAll(Collection<? extends E> c) {
        final AddAllOperation operation = new AddAllOperation(name, getDataList(c));
        return (Boolean)invoke(operation);
    }

    public boolean removeAll(Collection<?> c) {
        CompareAndRemoveOperation operation = new CompareAndRemoveOperation(name, getDataList(c), false);
        return (Boolean)invoke(operation);
    }

    public boolean retainAll(Collection<?> c) {
        CompareAndRemoveOperation operation = new CompareAndRemoveOperation(name, getDataList(c), true);
        return (Boolean)invoke(operation);
    }

    public void clear() {
        final ClearOperation operation = new ClearOperation(name);
        invoke(operation);
    }

//    public LocalQueueStats getLocalQueueStats() {
//        return null;
//    }
    //TODO stats

    private <T> T invoke(QueueOperation operation){
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(PriorityQueueService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private void throwExceptionIfNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object is null");
        }
    }

    private List<Data> listInternal() {
        IteratorOperation operation = new IteratorOperation(name);
        SerializableCollection collectionContainer = invoke(operation);
        return (List<Data>) collectionContainer.getCollection();
    }

    private List<Data> getDataList(Collection<?> objects) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            dataList.add(nodeEngine.toData(o));
        }
        return dataList;
    }
}