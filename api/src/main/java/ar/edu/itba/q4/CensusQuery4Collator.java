package ar.edu.itba.q4;

import ar.edu.itba.Region;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CensusQuery4Collator implements Collator<Map.Entry<Region, Integer>, Map<Region, Integer>> {

    @Override
    public Map<Region, Integer> collate(Iterable<Map.Entry<Region, Integer>> values) {
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
