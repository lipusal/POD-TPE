package ar.edu.itba.q6;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CensusQuery6CombinerFactory implements CombinerFactory<String, Integer, Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new CensusQuery6Combiner();
    }

    private class CensusQuery6Combiner extends Combiner<Integer,Integer>{

        private int count=0;
        @Override
        public void combine(Integer value) {
            count = count + value;
        }

        @Override
        public Integer finalizeChunk() {
            return count;
        }

        public void reset(){
            count = 0;
        }
    }
}

