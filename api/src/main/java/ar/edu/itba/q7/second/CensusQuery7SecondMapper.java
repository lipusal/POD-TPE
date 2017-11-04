package ar.edu.itba.q7.second;

import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Set;

public class CensusQuery7SecondMapper implements Mapper<String, Set<String>, Tuple, Integer> {
    @Override
    public void map(String s, Set<String> strings, Context<Tuple, Integer> context) {
        String[] provinces = strings.toArray(new String[0]);

        for (int i = 0; i < provinces.length - 1; i++) {
            for (int j = i+1; j < provinces.length; j++) {
                Tuple tuple = new Tuple(provinces[i], provinces[j]);
                context.emit(tuple, 1);
            }
        }
    }
}
