package ar.edu.itba.example;

import ar.edu.itba.CensusEntry;
import com.hazelcast.mapreduce.Context;

public class IdentityMapper implements com.hazelcast.mapreduce.Mapper<String, CensusEntry, String, CensusEntry> {
    @Override
    public void map(String s, CensusEntry censusEntry, Context<String, CensusEntry> context) {
        context.emit(s, censusEntry);
    }
}
