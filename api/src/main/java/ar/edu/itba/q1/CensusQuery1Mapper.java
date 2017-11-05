package ar.edu.itba.q1;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;

public class CensusQuery1Mapper implements com.hazelcast.mapreduce.Mapper<String, CensusEntry, Region, Integer> {

    public CensusQuery1Mapper() {
        System.out.println("Hello from query 1 mapper");
    }

    @Override
    public void map(String key, CensusEntry censusEntry, Context<Region, Integer> context) {
        System.out.println("Emitting from query q mapper");
        context.emit(censusEntry.getRegion(), 1);
    }
}
