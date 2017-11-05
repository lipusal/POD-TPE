package ar.edu.itba.q6;

import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery6Mapper implements Mapper<Long, Tuple<String, String>, String, String>{

    @Override
    public void map(Long aLong, Tuple<String, String> departmentProvinceTuple, Context<String, String> context) {
        context.emit(departmentProvinceTuple.getFirst(), departmentProvinceTuple.getSecond());
    }
}
