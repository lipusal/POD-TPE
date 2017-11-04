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
    private Region region;
    
    public CensusEntry() {}

    public CensusEntry(Status status, long homeId, String department, String province) {
        this.status = status;
        this.homeId = homeId;
        this.department = department;
        this.province = province;
        this.region = Region.fromString(province);
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

    public Region getRegion(){ return region; }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(status);
        objectDataOutput.writeLong(homeId);
        objectDataOutput.writeUTF(department);
        objectDataOutput.writeUTF(province);
        objectDataOutput.writeObject(region);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.status = objectDataInput.readObject();
        this.homeId = objectDataInput.readLong();
        this.department = objectDataInput.readUTF();
        this.province = objectDataInput.readUTF();
        this.region = objectDataInput.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CensusEntry that = (CensusEntry) o;
        return homeId == that.homeId && status == that.status && department.equals(that.department) && province.equals(that.province);
    }

    @Override
    public int hashCode() {
        int result = status.hashCode();
        result = 31 * result + (int) (homeId ^ (homeId >>> 32));
        result = 31 * result + department.hashCode();
        result = 31 * result + province.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CensusEntry{" +
                "status=" + status +
                ", homeId=" + homeId +
                ", department='" + department + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
