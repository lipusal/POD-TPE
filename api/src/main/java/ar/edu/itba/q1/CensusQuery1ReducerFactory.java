package ar.edu.itba.q1;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery1ReducerFactory implements ReducerFactory<Region, Integer, Integer>{
    @Override
    public Reducer<Integer, Integer> newReducer(Region region) {
        return new CensusQuery1Reducer();
    }

    private class CensusQuery1Reducer extends Reducer<Integer,Integer> {
        private int count = 0;

        @Override
        public void reduce(Integer value) {
            count += value;
        }

        @Override
        public Integer finalizeReduce() {
            return count;
        }
    }
}
