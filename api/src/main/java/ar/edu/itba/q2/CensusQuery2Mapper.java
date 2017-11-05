package ar.edu.itba.q2;

import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;

public class CensusQuery2Mapper implements com.hazelcast.mapreduce.Mapper<Long, Tuple<String, String>, String, Integer> {
    private final String prov;

    public CensusQuery2Mapper(String searchedProvince) {
        this.prov = searchedProvince;
    }

    @Override
    public void map(Long unusedKey, Tuple<String, String> departmentProvinceTuple, Context<String, Integer> context) {
        if(departmentProvinceTuple.getSecond().equals(prov)) {
            context.emit(departmentProvinceTuple.getFirst(), 1);
        }
    }
}
