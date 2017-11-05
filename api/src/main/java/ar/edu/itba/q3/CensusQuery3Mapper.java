package ar.edu.itba.q3;


import ar.edu.itba.Region;
import ar.edu.itba.Status;
import ar.edu.itba.Tuple;
import com.hazelcast.mapreduce.Context;

public class CensusQuery3Mapper implements com.hazelcast.mapreduce.Mapper<Long, Tuple<Region, Status>, Region, Integer> {

    public CensusQuery3Mapper() {
    }

    @Override
    public void map(Long aLong, Tuple<Region, Status> regionStatusTuple, Context<Region, Integer> context) {
        Region region = regionStatusTuple.getFirst();
        Status status = regionStatusTuple.getSecond();

        if(status == Status.EMPLOYED) {
            context.emit(region, 0);
        } else if(status == Status.UNEMPLOYED) {
            context.emit(region, 1);
        }
    }
}
