package ar.edu.itba.q5;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;

public class CensusQuery5Mapper implements com.hazelcast.mapreduce.Mapper<String, CensusEntry, Region, Long> {
    @Override
    public void map(String s, CensusEntry censusEntry, Context<Region, Long> context) {
        context.emit(censusEntry.getRegion(), censusEntry.getHomeId());
    }
}