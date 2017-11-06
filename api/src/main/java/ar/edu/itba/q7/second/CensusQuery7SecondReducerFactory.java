package ar.edu.itba.q7.second;

import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CensusQuery7SecondReducerFactory implements ReducerFactory<Tuple<String, String>, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(Tuple<String, String> provinceTuple) {
        return new Reducer<Integer, Integer>() {

            private Integer counter = 0;

            @Override
            public void reduce(Integer integer) {
                counter += integer;
            }

            @Override
            public Integer finalizeReduce() {
                return counter;
            }
        };
    }
}
