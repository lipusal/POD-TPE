package ar.edu.itba;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class LongTuple implements DataSerializable {

    private long key;
    private long value;

    public LongTuple(long key, long value) {
        this.key = key;
        this.value = value;
    }

    public LongTuple() { }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeLong(this.getKey());
        objectDataOutput.writeLong(this.getValue());
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.key = objectDataInput.readLong();
        this.value = objectDataInput.readLong();
    }
}
