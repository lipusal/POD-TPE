package ar.edu.itba.q5;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CensusQuery5ReducerFactory implements ReducerFactory<Region, Long, Double> {

    @Override
    public Reducer<Long, Double> newReducer(Region region) {
        return new Reducer<Long, Double>() {

            private Map<Long, Integer> homeIdsCounter = new HashMap<>();

            @Override
            public void reduce(Long homeId) {
                homeIdsCounter.merge(homeId, 1, (a, b) -> a + b);
            }

            @Override
            public Double finalizeReduce() {
                return homeIdsCounter.values().stream().collect(Collectors.averagingDouble(o -> o));
            }
        };
    }
}
