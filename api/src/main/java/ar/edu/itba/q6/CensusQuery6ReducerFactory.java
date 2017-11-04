package ar.edu.itba.q6;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery6ReducerFactory implements ReducerFactory<String, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String key) {
        return new CensusQuery6Reducer();
    }

    private class CensusQuery6Reducer extends Reducer<Integer,Integer> {
        private int count;

        @Override
        public void reduce(Integer value) {
            count+=value;
        }

        @Override
        public Integer finalizeReduce() {
            return count;
        }
    }
}
