/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.gurbuz.hazel.priorityqueue;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * @author ali 2/12/13
 */
public class BasicQueueTest {

    @Before
    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        Config config = createConfig();
        final String name = "queue";
        final QueueConfig queueConfig = config.getQueueConfig(name);
        final DummyListener dummyListener = new DummyListener();
        final ItemListenerConfig itemListenerConfig = new ItemListenerConfig(dummyListener, true);
        queueConfig.addItemListenerConfig(itemListenerConfig);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final IQueue queue = instance.getQueue(name);
        queue.offer("item");
        queue.poll();
        assertTrue(dummyListener.latch.await(10, TimeUnit.SECONDS));
    }

    private static class DummyListener implements ItemListener, Serializable {

        public final CountDownLatch latch = new CountDownLatch(2);

        public DummyListener() {
        }

        public void itemAdded(ItemEvent item) {
            latch.countDown();
        }

        public void itemRemoved(ItemEvent item) {
            latch.countDown();
        }
    }

    @Test
    @Ignore
    public void testQueueEviction() throws Exception {
        final Config config = createConfig();
        config.getQueueConfig("q").setEmptyQueueTtl(2);
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        final IQueue<Object> q = hz.getDistributedObject(PriorityQueueService.SERVICE_NAME, "q");

        try {
            assertTrue(q.offer("item"));
            assertEquals("item", q.poll());
            q.take();
            fail();
        } catch (Exception e){
            assertTrue(e instanceof DistributedObjectDestroyedException);
        }
        q.size();

    }

    @Test
    @Ignore
    public void testQueueEviction2() throws Exception {
        final Config config = new Config();
        config.getQueueConfig("q2").setEmptyQueueTtl(0);
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        final IQueue<Object> q = hz.getQueue("q2");
        q.offer("item");
        q.poll();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }


    @Test
    @Ignore
    public void testQueueStats() {
        Config config = new Config();
        final String name = "t_queue";

        HazelcastInstance ins1 = Hazelcast.newHazelcastInstance(config);
        final int items = 20;
        IQueue q1 = ins1.getQueue(name);
        for (int i = 0; i < items / 2; i++) {
            q1.offer("item" + i);
        }

        HazelcastInstance ins2 = Hazelcast.newHazelcastInstance(config);
        IQueue q2 = ins2.getQueue(name);
        for (int i = 0; i < items / 2; i++) {
            q2.offer("item" + i);
        }

        LocalQueueStats stats1 = ins1.getQueue(name).getLocalQueueStats();
        LocalQueueStats stats2 = ins2.getQueue(name).getLocalQueueStats();

        assertTrue(stats1.getOwnedItemCount() == items || stats2.getOwnedItemCount() == items);
        assertFalse(stats1.getOwnedItemCount() == items && stats2.getOwnedItemCount() == items);

        if (stats1.getOwnedItemCount() == items) {
            assertEquals(items, stats2.getBackupItemCount());
            assertEquals(0, stats1.getBackupItemCount());
        } else {
            assertEquals(items, stats1.getBackupItemCount());
            assertEquals(0, stats2.getBackupItemCount());
        }
    }

    @Test
    public void testOfferPoll() throws Exception {
        Config config = createConfig();
        final int count = 9;
        final String name = "defQueue";
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final Random rnd = new Random(System.currentTimeMillis());
        IQueue<String> q = instance1.getDistributedObject(PriorityQueueService.SERVICE_NAME, name);

        for (int i = 0; i < count; i++) {
            int index = rnd.nextInt(2);
            if (index == 0){
                IQueue<String> queue = instance1.getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
                queue.offer("item" + i);
            } else {
                IQueue<String> queue = instance2.getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
                queue.offer("item" + i);
            }
        }

        assertEquals(count, q.size());

        for (int i = 0; i < count; i++) {
            int index = rnd.nextInt(2);
            if (index == 0){
                IQueue<String> queue = instance2.getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
                String item = queue.poll();
                assertEquals(item, "item" + i);
            } else {
                IQueue<String> queue = instance1.getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
                String item = queue.poll();
                assertEquals(item, "item" + i);
            }
        }
        assertEquals(0, q.size());
        assertNull(q.poll());

    }

    @Test
    public void testOfferPollWithTimeout() throws Exception {
        final String name = "defQueue";
        Config config = createConfig();
        final int count = 9;
        config.getQueueConfig(name).setMaxSize(count);
        final int insCount = 2;
        final HazelcastInstance[] instances = {Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};
        final IQueue<String> q = instances[0].getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
        final Random rnd = new Random(System.currentTimeMillis());

        for (int i = 0; i < count; i++) {
            int index = rnd.nextInt(insCount);
            IQueue<String> queue = instances[index].getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
            queue.offer("item" + i);
        }

        assertFalse(q.offer("rejected", 1, TimeUnit.SECONDS));
        assertEquals("item0", q.poll());
        assertTrue(q.offer("not rejected", 1, TimeUnit.SECONDS));


        new Thread() {
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.poll();
            }
        }.start();
        assertTrue(q.offer("not rejected", 5, TimeUnit.SECONDS));

        assertEquals(count, q.size());

        for (int i = 0; i < count; i++) {
            int index = rnd.nextInt(insCount);
            IQueue<String> queue = instances[index].getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
            queue.poll();
        }

        assertNull(q.poll(1, TimeUnit.SECONDS));
        assertTrue(q.offer("offered1"));
        assertEquals("offered1", q.poll(1, TimeUnit.SECONDS));


        new Thread() {
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.offer("offered2");
            }
        }.start();
        assertEquals("offered2", q.poll(5, TimeUnit.SECONDS));

        assertEquals(0, q.size());
    }

    @Test
    public void removeAndContains() {
        final String name = "defQueue";
        Config config = createConfig();
        final int count = 10;
        config.getQueueConfig(name).setMaxSize(count);
        final HazelcastInstance[] instances = {Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};

        for (int i = 0; i < count; i++) {
            getPriorityQueue(instances, name).offer("item" + i);
        }

        assertTrue(getPriorityQueue(instances, name).contains("item4"));
        assertFalse(getPriorityQueue(instances, name).contains("item10"));
        assertTrue(getPriorityQueue(instances, name).remove("item4"));
        assertFalse(getPriorityQueue(instances, name).contains("item4"));
        assertEquals(getPriorityQueue(instances, name).size(), 9);

        List<String> list = new ArrayList<String>(3);
        list.add("item1");
        list.add("item2");
        list.add("item3");

        assertTrue(getPriorityQueue(instances, name).containsAll(list));
        list.add("item4");
        assertFalse(getPriorityQueue(instances, name).containsAll(list));
    }

    @Test
    public void testDrainAndIterator() {
        final String name = "defQueue";
        Config config = createConfig();
        final int count = 10;
        config.getQueueConfig(name).setMaxSize(count);
        final HazelcastInstance[] instances = {Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};

        for (int i = 0; i < count; i++) {
            getPriorityQueue(instances, name).offer("item" + i);
        }
        Iterator iter = getPriorityQueue(instances, name).iterator();
        int i = 0;
        while (iter.hasNext()) {
            Object o = iter.next();
            assertEquals(o, "item" + i++);
        }

        Object[] array = getPriorityQueue(instances, name).toArray();
        for (i = 0; i < array.length; i++) {
            Object o = array[i];
            assertEquals(o, "item" + i++);
        }

        String[] arr = new String[5];
        IQueue<String> q = getPriorityQueue(instances, name);
        arr = q.toArray(arr);
        assertEquals(arr.length, 10);
        for (i = 0; i < arr.length; i++) {
            Object o = arr[i];
            assertEquals(o, "item" + i++);
        }


        List list = new ArrayList(4);
        getPriorityQueue(instances, name).drainTo(list, 4);

        assertEquals(list.remove(0), "item0");
        assertEquals(list.remove(0), "item1");
        assertEquals(list.remove(0), "item2");
        assertEquals(list.remove(0), "item3");
        assertEquals(list.size(), 0);

        getPriorityQueue(instances, name).drainTo(list);
        assertEquals(list.size(), 6);
        assertEquals(list.remove(0), "item4");

    }

    @Test
    public void testAddRemoveRetainAll() {
        final String name = "defQueue";
        Config config = createConfig();
        final int count = 10;
        config.getQueueConfig(name).setMaxSize(count);
        final HazelcastInstance[] instances = {Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};

        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            list.add("item" + i);
        }

        assertTrue(getPriorityQueue(instances, name).addAll(list));
        assertEquals(getPriorityQueue(instances, name).size(), 10);

        List<String> arrayList = new ArrayList<String>();


        arrayList.add("item3");
        arrayList.add("item4");
        arrayList.add("item31");
        assertTrue(getPriorityQueue(instances, name).retainAll(arrayList));
        assertEquals(getPriorityQueue(instances, name).size(), 2);

        arrayList.clear();
        arrayList.add("item31");
        arrayList.add("item34");
        assertFalse(getPriorityQueue(instances, name).removeAll(arrayList));

        arrayList.clear();
        arrayList.add("item3");
        arrayList.add("item4");
        arrayList.add("item12");
        assertTrue(getPriorityQueue(instances, name).removeAll(arrayList));

        assertEquals(getPriorityQueue(instances, name).size(), 0);
    }

    @Test
    public void testListeners() throws InterruptedException {
        final String name = "defQueue";
        Config config = createConfig();
        final int count = 100;
        config.getQueueConfig(name).setMaxSize(count);
        final HazelcastInstance[] instances = {Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};
        final CountDownLatch latch = new CountDownLatch(20);
        final AtomicBoolean notCalled = new AtomicBoolean(true);

        IQueue q = getPriorityQueue(instances, name);
        ItemListener listener = new ItemListener() {
            int offer;

            int poll;

            public void itemAdded(ItemEvent item) {
                if (item.getItem().equals("item" + offer++)) {
                    latch.countDown();
                } else {
                    notCalled.set(false);
                }
            }

            public void itemRemoved(ItemEvent item) {
                if (item.getItem().equals("item" + poll++)) {
                    latch.countDown();
                } else {
                    notCalled.set(false);
                }
            }
        };
        final String id = q.addItemListener(listener, true);

        for (int i = 0; i < 10; i++) {
            getPriorityQueue(instances, name).offer("item" + i);
        }
        for (int i = 0; i < 10; i++) {
            getPriorityQueue(instances, name).poll();
        }
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        q.removeItemListener(id);
        getPriorityQueue(instances, name).offer("item-a");
        getPriorityQueue(instances, name).poll();
        Thread.sleep(2 * 1000);
        assertTrue(notCalled.get());
    }

    private IQueue getPriorityQueue(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random(System.currentTimeMillis());
        return instances[rnd.nextInt(instances.length)].getDistributedObject(PriorityQueueService.SERVICE_NAME, name);
    }

    private Config createConfig(){
        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true);
        serviceConfig.setClassName(PriorityQueueService.class.getName());
        serviceConfig.setName(PriorityQueueService.SERVICE_NAME);

        final Config config = new Config();
        final ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(serviceConfig);
        return config;
    }


}
