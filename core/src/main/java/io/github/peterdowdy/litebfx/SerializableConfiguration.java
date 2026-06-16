package io.github.peterdowdy.litebfx;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wraps Hadoop {@link Configuration} for Java serialization.
 *
 * Configuration internally holds a ClassLoader reference which is not
 * serializable. This wrapper uses Configuration's own Writable encoding
 * (write/readFields) which serializes only the key-value properties.
 */
public class SerializableConfiguration implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SerializableConfiguration.class);

    private transient Configuration value;

    public SerializableConfiguration(Configuration conf) {
        log.trace("SerializableConfiguration(conf)");
        this.value = conf;
    }

    public Configuration get() {
        log.trace("get()");
        return value;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        log.trace("writeObject()");
        out.defaultWriteObject();
        value.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        log.trace("readObject()");
        in.defaultReadObject();
        value = new Configuration(false);
        value.readFields(in);
    }
}
