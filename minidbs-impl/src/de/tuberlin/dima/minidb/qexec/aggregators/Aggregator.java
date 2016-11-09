package de.tuberlin.dima.minidb.qexec.aggregators;

import de.tuberlin.dima.minidb.core.DataField;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public interface Aggregator {


        void initializeAggregate();

        void aggregateField(DataField dataField);

        DataField finalizeAggregate();


}
