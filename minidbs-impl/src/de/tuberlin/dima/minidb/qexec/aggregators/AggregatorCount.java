package de.tuberlin.dima.minidb.qexec.aggregators;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.IntField;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class AggregatorCount implements Aggregator {



    private int count;


    public void initializeAggregate() {
        count = 0;
    }

    public void aggregateField(DataField field) {
        count++;
    }

    public IntField finalizeAggregate() {
        return new IntField(count);
    }


}




