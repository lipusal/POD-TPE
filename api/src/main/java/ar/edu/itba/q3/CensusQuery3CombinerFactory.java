package ar.edu.itba.q3;

import ar.edu.itba.LongTuple;
import ar.edu.itba.Region;
import ar.edu.itba.Status;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.awt.*;

public class CensusQuery3CombinerFactory implements CombinerFactory<Region, Integer, LongTuple>{
    @Override
    public Combiner<Integer, LongTuple> newCombiner(Region status) {
        return new CensusQuery3Combiner();
    }

    private class CensusQuery3Combiner extends Combiner<Integer,LongTuple>{

        private long numerator = 0;
        private long denominator = 0;

        @Override
        public void combine(Integer value) {
            denominator++;
            numerator += value;
        }

        @Override
        public LongTuple finalizeChunk() {
            return new LongTuple(numerator, denominator);
        }

        public void reset(){
            numerator = 0;
            denominator = 0;
        }
    }

}
