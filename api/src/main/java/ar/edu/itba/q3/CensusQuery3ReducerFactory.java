package ar.edu.itba.q3;


import ar.edu.itba.Region;
import ar.edu.itba.Status;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.awt.*;

public class CensusQuery3ReducerFactory implements ReducerFactory<Region,Point,Double> {


    @Override
    public Reducer<Point, Double> newReducer(Region region) {
        return new CensusQuery3Reducer();
    }


    private class CensusQuery3Reducer extends Reducer<Point, Double> {
        private volatile double numerator;
        private volatile double denominator;

        //Is it neccesary to use beginReduce method?
        @Override
        public void beginReduce() {
            numerator = 0;
            denominator = 0;
        }

        @Override
        public void reduce(Point value) {
            numerator += value.x;
            denominator += value.y;
        }

        @Override
        public Double finalizeReduce() {
            return numerator / denominator;
        }
    }
}