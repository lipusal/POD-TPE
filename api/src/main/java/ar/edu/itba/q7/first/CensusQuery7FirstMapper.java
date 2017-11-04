package ar.edu.itba.q7.first;

import ar.edu.itba.CensusEntry;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CensusQuery7FirstMapper implements Mapper<String, CensusEntry, String, String> {
    @Override
    public void map(String s, CensusEntry censusEntry, Context<String, String> context) {
        context.emit(censusEntry.getDepartment(), censusEntry.getProvince());
    }
}
