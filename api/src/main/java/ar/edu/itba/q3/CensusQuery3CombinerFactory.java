package ar.edu.itba.q3;

import ar.edu.itba.Region;
import ar.edu.itba.Status;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.awt.*;

public class CensusQuery3CombinerFactory implements CombinerFactory<Region, Integer, Point>{
    @Override
    public Combiner<Integer, Point> newCombiner(Region status) {
        return new CensusQuery3Combiner();
    }

    private class CensusQuery3Combiner extends Combiner<Integer,Point>{

        private int numerator = 0;
        private int denominator = 0;

        @Override
        public void combine(Integer value) {
            denominator++;
            numerator += value;
        }

        @Override
        public Point finalizeChunk() {
            return new Point(numerator, denominator);
        }

        public void reset(){
            numerator = 0;
            denominator = 0;
        }
    }

}
