package ar.edu.itba.q7.first;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery7FirstMapper implements Mapper<Long, Tuple<String, String>, String, String> {
    @Override
    public void map(Long s, Tuple<String, String> stringStringTuple, Context<String, String> context) {
        context.emit(stringStringTuple.getFirst(), stringStringTuple.getSecond());
    }
}
