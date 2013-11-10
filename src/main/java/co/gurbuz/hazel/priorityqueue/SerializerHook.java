package co.gurbuz.hazel.priorityqueue;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @ali 05/11/13
 */
//TODO not used, why?
public class SerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId("hazelcast.serialization.priority.queue", -81);

    static final int CONTAINER = 1;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId){
                    case CONTAINER:
                        System.err.println("asdf aloooo");
                        return new PriorityQueueContainer(null);
                }
                return null;
            }
        };
    }
}
