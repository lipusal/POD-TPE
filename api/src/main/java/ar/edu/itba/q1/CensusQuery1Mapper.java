package ar.edu.itba.q1;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;

/**
 * Created by root on 10/31/17.
 */
public class CensusQuery1Mapper implements com.hazelcast.mapreduce.Mapper<String, CensusEntry, Region, Integer> {
    @Override
    public void map(String key, CensusEntry censusEntry, Context<Region, Integer> context) {
        context.emit(censusEntry.getRegion(), 1);
    }
}
