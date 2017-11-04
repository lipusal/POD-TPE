package ar.edu.itba.q7.second;

import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CensusQuery7SecondCollator implements Collator<Map.Entry<Tuple, Integer>, Map<String, Integer>> {

    private Integer limit;

    public CensusQuery7SecondCollator(Integer limit) {
        this.limit = limit;
    }

    @Override
    public Map<String, Integer> collate(Iterable<Map.Entry<Tuple, Integer>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .filter(counterEntry -> counterEntry.getValue() >= limit)
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Object::toString,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,         // TODO check if this is correct
                        LinkedHashMap::new      // Default is a HashMap which doesn't maintain key order
                ));
    }
}
