package ar.edu.itba.q2;

import ar.edu.itba.CensusEntry;
import com.hazelcast.mapreduce.Context;

public class CensusQuery2Mapper implements com.hazelcast.mapreduce.Mapper<String, CensusEntry, String, Integer> {

    String prov;

    public CensusQuery2Mapper(String searchedProvince){
        this.prov = searchedProvince;
    }

    @Override
    public void map(String s, CensusEntry censusEntry, Context<String, Integer> context) {
        String provinceName = censusEntry.getProvince();
        String departmentName = censusEntry.getDepartment();

        if(provinceName.equals(prov)){
            context.emit(departmentName, 1);
        }
    }
}
