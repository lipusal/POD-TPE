package ar.edu.itba.q4;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery4Mapper implements Mapper<String, CensusEntry, Region, Long> {
    @Override
    public void map(String s, CensusEntry censusEntry, Context<Region, Long> context) {
        context.emit(censusEntry.getRegion(), censusEntry.getHomeId());
    }
}
