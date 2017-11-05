package ar.edu.itba.q7.second;

import ar.edu.itba.ProvinceTuple;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Set;

public class CensusQuery7SecondMapper implements Mapper<String, Set<String>, ProvinceTuple, Integer> {
    @Override
    public void map(String s, Set<String> strings, Context<ProvinceTuple, Integer> context) {
        String[] provinces = strings.toArray(new String[0]);

        for (int i = 0; i < provinces.length - 1; i++) {
            for (int j = i+1; j < provinces.length; j++) {
                ProvinceTuple provinceTuple = new ProvinceTuple(provinces[i], provinces[j]);
                context.emit(provinceTuple, 1);
            }
        }
    }
}
