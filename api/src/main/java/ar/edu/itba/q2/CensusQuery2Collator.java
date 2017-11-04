package ar.edu.itba.q2;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by root on 11/4/17.
 */
public class CensusQuery2Collator implements Collator<Map.Entry<String,Integer>,List<Map.Entry<String,Integer>>>{

    int limit;

    public CensusQuery2Collator(int limit){
        this.limit = limit;
    }

    @Override
    public List<Map.Entry<String, Integer>> collate(Iterable<Map.Entry<String, Integer>> values) {
        List<Map.Entry<String,Integer>> aux = new ArrayList<>();

        for(Map.Entry<String,Integer> value: values){
            aux.add(value);
        }
        Collections.sort(aux, new IntegerComparator());

        return aux.subList(0,limit);
    }

    private static class IntegerComparator implements Comparator<Map.Entry<String,Integer>>{

        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return Integer.compare(o2.getValue(),o1.getValue());
        }
    }
}
