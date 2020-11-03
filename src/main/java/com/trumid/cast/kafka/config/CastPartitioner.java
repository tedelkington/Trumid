package com.trumid.cast.kafka.config;

import com.trumid.cast.data.CastKey;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * {@link com.trumid.cast.kafka.config.Topics#Casts} has 3 partitions
 * 0 for originatorUserId == 0 (prioritized client) and the rest are distributed by odd/even
 *
 * Just to illustrate we could have a complex allocation rule
 */
public final class CastPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return partition((CastKey)key); // we could check the bytes instead  but since the object is already materialized
    }

    public boolean isForInstance(int instanceId, CastKey castKey) {
        return instanceId == partition(castKey);
    }

    private int partition(CastKey castKey) {
        if (0 == castKey.originatorUserId) {
            return 0;
        }

        return castKey.originatorUserId % 2 == 0 ? 2 : 1;
    }

    @Override public void close() { }
    @Override public void configure(Map<String, ?> configs) { }
}
