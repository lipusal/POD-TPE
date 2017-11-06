package ar.edu.itba.q7.second;

import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Set;

public class CensusQuery7SecondMapper implements Mapper<String, Set<String>, Tuple<String, String>, Integer> {
    @Override
    public void map(String s, Set<String> strings, Context<Tuple<String, String>, Integer> context) {
        String[] provinces = strings.toArray(new String[0]);

        for (int i = 0; i < provinces.length - 1; i++) {
            for (int j = i+1; j < provinces.length; j++) {
                context.emit(new Tuple<>(provinces[i], provinces[j]), 1);
            }
        }
    }
}
