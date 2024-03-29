package ar.edu.itba.q6;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CensusQuery6Collator implements Collator<Map.Entry<String, Integer>, Map<String, Integer>> {
    private final int min;

    public CensusQuery6Collator(int min){
        this.min = min;
    }

    @Override
    public Map<String, Integer> collate(Iterable<Map.Entry<String, Integer>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .filter(stringIntegerEntry -> stringIntegerEntry.getValue() >= min)
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,         // TODO check if this is correct
                        LinkedHashMap::new      // Default is a HashMap which doesn't maintain key order
                ));
    }
}
