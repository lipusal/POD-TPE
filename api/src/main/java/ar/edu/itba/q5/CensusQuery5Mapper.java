package ar.edu.itba.q5;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;

public class CensusQuery5Mapper implements com.hazelcast.mapreduce.Mapper<Long, Tuple<Region, Long>, Region, Long> {
    @Override
    public void map(Long s, Tuple<Region, Long> regionLongTuple, Context<Region, Long> context) {
        context.emit(regionLongTuple.getFirst(), regionLongTuple.getSecond());
    }
}
