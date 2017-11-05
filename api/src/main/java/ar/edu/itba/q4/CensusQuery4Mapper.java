package ar.edu.itba.q4;

import ar.edu.itba.Region;
import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery4Mapper implements Mapper<Long, Tuple<Region, Long>, Region, Long> {
    @Override
    public void map(Long aLong, Tuple<Region, Long> regionLongTuple, Context<Region, Long> context) {
        context.emit(regionLongTuple.getFirst(), regionLongTuple.getSecond());
    }
}
