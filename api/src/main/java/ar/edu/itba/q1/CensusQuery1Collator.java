package ar.edu.itba.q1;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class CensusQuery1Collator implements Collator<Map.Entry<Region, Integer>, Map<Region, Integer>> {

    @Override
    public Map<Region, Integer> collate(Iterable<Map.Entry<Region, Integer>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,         // TODO check if this is correct
                        LinkedHashMap::new      // Default is a HashMap which doesn't maintain key order
                ));
    }
}
