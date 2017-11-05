package ar.edu.itba.q6;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery6Mapper implements Mapper<Long, String, String, Integer>{

    @Override
    public void map(Long key, String department, Context<String, Integer> context) {
        context.emit(department,1);
    }
}
