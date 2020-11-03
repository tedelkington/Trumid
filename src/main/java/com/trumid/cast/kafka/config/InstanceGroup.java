package com.trumid.cast.kafka.config;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;

/**
 * Simple representation of which instance id i am out of a total
 */
public final class InstanceGroup {
    public final int instanceId = systemPropertyInt("instanceId");

    private final int numberOfInstances = systemPropertyInt("numberOfInstances");

    public boolean isMine(int key) {
        return instanceId == key % numberOfInstances;
    }

    private static int systemPropertyInt(String name) {
        final String property = getProperty(name);
        if (property == null) {
            throw new IllegalArgumentException("System property " + name + " not set");
        }

        return parseInt(property);
    }

    @Override
    public String toString() {
        return "InstanceGroup{" +
                "instanceId=" + instanceId +
                ", numberOfInstances=" + numberOfInstances +
                '}';
    }
}
