package de.tuberlin.dima.minidb.qexec.aggregators;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class AggregatorMax implements Aggregator {

    private DataType dataType;
    private DataField max;

    public AggregatorMax(DataType dataType) {
        this.dataType = dataType;
    }

    public void initializeAggregate() {
        max = null;
    }

    public void aggregateField(DataField currentField) {
        DataField tempMax;
        label:
        {
            if (max != null) {
                if (currentField.isNULL()) {
                    tempMax = max;
                    break label;
                }

                if (currentField.compareTo(max) > 0) {
                } else {
                    tempMax = max;
                    break label;
                }
            } else {
                if (currentField.isNULL()) {
                    tempMax = null;
                    break label;
                }
            }

            tempMax = currentField;
        }

        max = tempMax;
    }

    public DataField finalizeAggregate() {
        return max == null ? dataType.getNullValue() : max;
    }


}
