package ar.edu.itba.q1;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;

/**
 * Created by root on 10/31/17.
 */
public class CensusQuery1Mapper implements com.hazelcast.mapreduce.Mapper<Region, CensusEntry, String, Integer> {

    @Override
    public void map(Region region, CensusEntry censusEntry, Context<String, Integer> context) {
        context.emit(region.toString(),1);
    }
}
