package ar.edu.itba;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

public class CensusEntry implements DataSerializable {
    private Status status;
    private long homeId;
    private String department;
    private String province;
    
    public CensusEntry() {}

    public CensusEntry(Status status, long homeId, String department, String province) {
        this.status = status;
        this.homeId = homeId;
        this.department = department;
        this.province = province;
    }

    public Status getStatus() {
        return status;
    }

    public long getHomeId() {
        return homeId;
    }

    public String getDepartment() {
        return department;
    }

    public String getProvince() {
        return province;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(status);
        objectDataOutput.writeLong(homeId);
        objectDataOutput.writeUTF(department);
        objectDataOutput.writeUTF(province);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.status = objectDataInput.readObject();
        this.homeId = objectDataInput.readLong();
        this.department = objectDataInput.readUTF();
        this.province = objectDataInput.readUTF();
    }
}
