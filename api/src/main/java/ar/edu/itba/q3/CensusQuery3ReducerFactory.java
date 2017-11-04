package ar.edu.itba.q3;


import ar.edu.itba.LongTuple;
import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery3ReducerFactory implements ReducerFactory<Region,LongTuple,Double> {


    @Override
    public Reducer<LongTuple, Double> newReducer(Region region) {
        return new CensusQuery3Reducer();
    }


    private class CensusQuery3Reducer extends Reducer<LongTuple, Double> {
        private volatile double numerator;
        private volatile double denominator;

        //Is it neccesary to use beginReduce method?
        @Override
        public void beginReduce() {
            numerator = 0;
            denominator = 0;
        }

        @Override
        public void reduce(LongTuple tuple) {
            numerator += tuple.getKey();
            denominator += tuple.getValue();
        }

        @Override
        public Double finalizeReduce() {
            return numerator / denominator;
        }
    }
}