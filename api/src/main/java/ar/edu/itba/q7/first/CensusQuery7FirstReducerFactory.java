package ar.edu.itba.q7.first;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class CensusQuery7FirstReducerFactory implements ReducerFactory<String, String, Set<String>>{
    @Override
    public Reducer<String, Set<String>> newReducer(String s) {
        return new Reducer<String, Set<String>>() {

            private Set<String> provinces = new HashSet<>();

            @Override
            public void reduce(String s) {
                provinces.add(s);
            }

            @Override
            public Set<String> finalizeReduce() {
                return provinces;
            }
        };
    }
}
