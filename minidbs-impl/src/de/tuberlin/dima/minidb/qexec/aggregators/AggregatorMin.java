package de.tuberlin.dima.minidb.qexec.aggregators;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class AggregatorMin implements Aggregator {
    private DataType dataType;
    private DataField min;

    public AggregatorMin(DataType dataType) {
        this.dataType = dataType;
    }

    public void initializeAggregate() {
        min = null;
    }

    public void aggregateField(DataField currentField) {
        DataField tempMin;
        label: {
            if(min != null) {
                if(currentField.isNULL()) {
                    tempMin = min;
                    break label;
                }

                if(currentField.compareTo(min) >= 0) {
                    tempMin = min;
                    break label;
                }
            } else if(currentField.isNULL()) {
                tempMin = null;
                break label;
            }

            tempMin = currentField;
        }

        min = tempMin;
    }

    public DataField finalizeAggregate() {
        return min == null ? dataType.getNullValue() : min;
    }


}
