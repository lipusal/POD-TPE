package ar.edu.itba.q4;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class RegionToHomeCountReducer implements ReducerFactory<Region, Long, Integer> {

    @Override
    public Reducer<Long, Integer> newReducer(Region region) {
        return new Reducer<Long, Integer>() {
            private Set<Long> uniqueHomeIds = new HashSet<>();

            @Override
            public void reduce(Long homeId) {
                uniqueHomeIds.add(homeId);
            }

            @Override
            public Integer finalizeReduce() {
                return uniqueHomeIds.size();
            }
        };
    }
}
