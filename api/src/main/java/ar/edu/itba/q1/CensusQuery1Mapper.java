package ar.edu.itba.q1;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;

public class CensusQuery1Mapper implements com.hazelcast.mapreduce.Mapper<Long, Region, Region, Integer> {

    @Override
    public void map(Long aLong, Region region, Context<Region, Integer> context) {
        context.emit(region, 1);
    }
}
