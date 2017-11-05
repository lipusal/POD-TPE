package ar.edu.itba.q6;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class CensusQuery6ReducerFactory implements ReducerFactory<String, String, Integer> {

    @Override
    public Reducer<String, Integer> newReducer(String key) {
        return new CensusQuery6Reducer();
    }

    private class CensusQuery6Reducer extends Reducer<String, Integer> {
        private Set<String> provinces = new HashSet<>();

        @Override
        public void reduce(String province) {
            provinces.add(province);
        }

        @Override
        public Integer finalizeReduce() {
            return provinces.size();
        }
    }
}
