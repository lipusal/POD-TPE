package ar.edu.itba.q5;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CensusQuery5Collator implements Collator<Map.Entry<Region, Double>, Map<Region, Double>> {
    @Override
    public Map<Region, Double> collate(Iterable<Map.Entry<Region, Double>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }
}
