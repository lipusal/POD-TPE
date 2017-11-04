package ar.edu.itba.q3;


import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.Status;
import com.hazelcast.mapreduce.Context;

public class CensusQuery3Mapper implements com.hazelcast.mapreduce.Mapper<String, CensusEntry, Region, Integer> {

    public CensusQuery3Mapper() {
    }

    @Override
    public void map(String s, CensusEntry censusEntry, Context<Region, Integer> context) {
        Region region = censusEntry.getRegion();
        Status status = censusEntry.getStatus();

        if(status.equals(Status.EMPLOYED)) {
            context.emit(region, 0);
        }
        else if(status.equals(Status.UNEMPLOYED)) {
            context.emit(region, 1);
        }
    }
}
