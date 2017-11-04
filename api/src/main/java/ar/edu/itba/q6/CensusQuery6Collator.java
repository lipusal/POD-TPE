package ar.edu.itba.q6;

import com.hazelcast.mapreduce.Collator;
import sun.applet.resources.MsgAppletViewer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CensusQuery6Collator implements Collator<Map.Entry<String, Integer>, Map<String, Integer>> {
    int limit;

    public CensusQuery6Collator(int limit){
        this.limit = limit;
    }

    @Override
    public Map<String, Integer> collate(Iterable<Map.Entry<String, Integer>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .filter(stringIntegerEntry -> stringIntegerEntry.getValue() >= limit)
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,         // TODO check if this is correct
                        LinkedHashMap::new      // Default is a HashMap which doesn't maintain key order
                ));
    }

    private static class IntegerComparator implements Comparator<Map.Entry<String,Integer>>{

        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return Integer.compare(o2.getValue(),o1.getValue());
        }
    }
}
