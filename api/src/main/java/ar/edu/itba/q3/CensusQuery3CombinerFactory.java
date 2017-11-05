package ar.edu.itba.q3;

import ar.edu.itba.Region;
import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CensusQuery3CombinerFactory implements CombinerFactory<Region, Integer, Tuple<Long, Long>>{
    @Override
    public Combiner<Integer, Tuple<Long, Long>> newCombiner(Region status) {
        return new CensusQuery3Combiner();
    }

    private class CensusQuery3Combiner extends Combiner<Integer, Tuple<Long, Long>>{

        private long numerator = 0;
        private long denominator = 0;

        @Override
        public void combine(Integer value) {
            denominator++;
            numerator += value;
        }

        @Override
        public Tuple<Long, Long> finalizeChunk() {
            return new Tuple<>(numerator, denominator);
        }

        public void reset() {
            numerator = 0;
            denominator = 0;
        }
    }

}
