package ar.edu.itba;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class ProvinceTuple implements DataSerializable {
    private String first;
    private String second;

    public ProvinceTuple(){

    }

    public ProvinceTuple(String first, String second) {
        if(first.compareTo(second) > 0){
            this.first = second;
            this.second = first;
        }else{
            this.first = first;
            this.second = second;
        }
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return first + " + " + second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProvinceTuple provinceTuple = (ProvinceTuple) o;

        if (first != null ? !first.equals(provinceTuple.first) : provinceTuple.first != null) return false;
        return second != null ? second.equals(provinceTuple.second) : provinceTuple.second == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeUTF(second);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readUTF();
    }
}
