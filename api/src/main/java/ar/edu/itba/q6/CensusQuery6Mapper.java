package ar.edu.itba.q6;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery6Mapper implements Mapper<Long, CensusEntry, String, Integer>{

    @Override
    public void map(Long key, CensusEntry censusEntry, Context<String, Integer> context) {
        context.emit(censusEntry.getDepartment(),1);
    }
}
