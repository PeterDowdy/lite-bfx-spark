package com.litebfx;

import org.apache.hadoop.conf.Configuration;

import java.io.*;

/**
 * Wraps Hadoop {@link Configuration} for Java serialization.
 *
 * Configuration internally holds a ClassLoader reference which is not
 * serializable. This wrapper uses Configuration's own Writable encoding
 * (write/readFields) which serializes only the key-value properties.
 */
class SerializableConfiguration implements Serializable {

    private transient Configuration value;

    SerializableConfiguration(Configuration conf) {
        this.value = conf;
    }

    Configuration get() {
        return value;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        value.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        value = new Configuration(false);
        value.readFields(in);
    }
}
