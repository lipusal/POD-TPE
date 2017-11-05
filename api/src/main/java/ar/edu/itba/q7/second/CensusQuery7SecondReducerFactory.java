package ar.edu.itba.q7.second;

import ar.edu.itba.ProvinceTuple;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery7SecondReducerFactory implements ReducerFactory<ProvinceTuple, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(ProvinceTuple provinceTuple) {
        return new Reducer<Integer, Integer>() {

            private Integer counter = 0;

            @Override
            public void reduce(Integer integer) {
                counter+=integer;
            }

            @Override
            public Integer finalizeReduce() {
                return counter;
            }
        };
    }
}
