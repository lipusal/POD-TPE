package ar.edu.itba.q2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery2ReducerFactory implements ReducerFactory<String,Integer,Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new CensusQuery2Reducer();
    }

    private class CensusQuery2Reducer extends Reducer<Integer,Integer> {
        private int count;

        //Is it neccesary to use beginReduce method?
        @Override
        public void beginReduce() {
            count = 0;
        }

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
