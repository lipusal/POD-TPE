package ar.edu.itba.q2;

import ar.edu.itba.CensusEntry;
import com.hazelcast.mapreduce.Context;

public class CensusQuery2Mapper implements com.hazelcast.mapreduce.Mapper<Long, CensusEntry, String, Integer> {

    private String prov;

    public CensusQuery2Mapper(String searchedProvince){
        this.prov = searchedProvince;
    }

    @Override
    public void map(Long s, CensusEntry censusEntry, Context<String, Integer> context) {
        String provinceName = censusEntry.getProvince();
        String departmentName = censusEntry.getDepartment();

        if(provinceName.equals(prov)){
            context.emit(departmentName, 1);
        }
    }
}
