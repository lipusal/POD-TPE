package ar.edu.itba.q2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CensusQuery2CombinerFactory implements CombinerFactory<String, Integer, Integer>{

    @Override
    public Combiner<Integer, Integer> newCombiner(String s) {
        return new CensusQuery2Combiner();
    }

    private class CensusQuery2Combiner extends Combiner<Integer,Integer>{
        private int count = 0;

        @Override
        public void combine(Integer value) {
            count += value;
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

