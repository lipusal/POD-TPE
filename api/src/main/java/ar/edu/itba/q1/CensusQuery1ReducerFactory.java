package ar.edu.itba.q1;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery1ReducerFactory implements ReducerFactory<Region, Integer, Integer> {

    public CensusQuery1ReducerFactory() {
        System.out.println("Hello from query 1 reducer factory");
    }

    @Override
    public Reducer<Integer, Integer> newReducer(Region region) {
        return new CensusQuery1Reducer();
    }

    private class CensusQuery1Reducer extends Reducer<Integer,Integer> {
        private int count;

        @Override
        public void reduce(Integer value) {
            System.out.println("Reducing from query 1 reducer");
            count++;
        }

        @Override
        public Integer finalizeReduce() {
            System.out.println("Finalizing reduce from query 1 reducer");
            return count;
        }
    }
}
