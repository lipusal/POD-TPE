package ar.edu.itba.q3;


import ar.edu.itba.Region;
import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery3ReducerFactory implements ReducerFactory<Region, Tuple<Long, Long>,Double> {


    @Override
    public Reducer<Tuple<Long, Long>, Double> newReducer(Region region) {
        return new CensusQuery3Reducer();
    }


    private class CensusQuery3Reducer extends Reducer<Tuple<Long, Long>, Double> {
        private double numerator;
        private double denominator;

        //Is it neccesary to use beginReduce method?
        @Override
        public void beginReduce() {
            numerator = 0;
            denominator = 0;
        }

        @Override
        public void reduce(Tuple<Long, Long> numeratorDenominatorTuple) {
            numerator += numeratorDenominatorTuple.getFirst();
            denominator += numeratorDenominatorTuple.getSecond();
        }

        @Override
        public Double finalizeReduce() {
            return numerator / denominator;
        }
    }
}
