package ar.edu.itba.example;

import ar.edu.itba.CensusEntry;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.LinkedList;
import java.util.List;

public class IdentityReducerFactory implements ReducerFactory<String, CensusEntry, List<CensusEntry>> {
    @Override
    public Reducer<CensusEntry, List<CensusEntry>> newReducer(String s) {
        return new IdentityReducer();
    }

    private class IdentityReducer extends Reducer<CensusEntry, List<CensusEntry>> {
        private List<CensusEntry> censusEntries = new LinkedList<>();

        @Override
        public void reduce(CensusEntry censusEntry) {
            this.censusEntries.add(censusEntry);
        }

        @Override
        public List<CensusEntry> finalizeReduce() {
            return censusEntries;
        }
    }
}
