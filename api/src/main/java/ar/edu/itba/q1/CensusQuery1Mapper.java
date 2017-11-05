package ar.edu.itba.q1;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;

public class CensusQuery1Mapper implements com.hazelcast.mapreduce.Mapper<Long, CensusEntry, Region, Integer> {
    @Override
    public void map(Long key, CensusEntry censusEntry, Context<Region, Integer> context) {
        context.emit(censusEntry.getRegion(), 1);
    }
}
