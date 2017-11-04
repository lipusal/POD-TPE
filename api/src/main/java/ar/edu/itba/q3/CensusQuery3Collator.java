package ar.edu.itba.q3;


import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CensusQuery3Collator implements Collator<Map.Entry<Region,Double>,Map<Region, Double>> {

    @Override
    public Map<Region, Double> collate(Iterable<Map.Entry<Region, Double>> values) {
        // Iterable -> Java 8 stream, https://stackoverflow.com/a/23936723/2333689
        // TODO: Parallel?
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

