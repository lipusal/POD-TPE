package ar.edu.itba.q6;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Created by lelv on 11/4/17.
 */
public class CensusQuery6Mapper implements Mapper<String, CensusEntry, String, Integer>{

    @Override
    public void map(String key, CensusEntry censusEntry, Context<String, Integer> context) {
        context.emit(censusEntry.getDepartment(),1);
    }
}
