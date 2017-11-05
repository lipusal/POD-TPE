package ar.edu.itba.q1;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CensusQuery1CombinerFactory implements CombinerFactory<Region, Integer, Integer>{

    @Override
    public Combiner<Integer, Integer> newCombiner(Region region) {
        return new CensusQuery1Combiner();
    }

    private class CensusQuery1Combiner extends Combiner<Integer, Integer> {
        private int count = 0;

        @Override
        public void combine(Integer integer) {
            count += integer;
        }

        @Override
        public Integer finalizeChunk() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }
}

