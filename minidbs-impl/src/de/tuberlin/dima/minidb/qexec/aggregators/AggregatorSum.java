package de.tuberlin.dima.minidb.qexec.aggregators;

import de.tuberlin.dima.minidb.core.ArithmeticType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class AggregatorSum implements Aggregator {

    private ArithmeticType<DataField> temp;
    private ArithmeticType<DataField> sum;

    public AggregatorSum(DataType dataType) {
        temp = DataType.asArithmeticType(dataType.getNullValue());
    }

    public void initializeAggregate() {
        sum = temp.createZero();
    }

    public void aggregateField(DataField currentField) {
        if(!currentField.isNULL()) {
            sum.add(currentField);
        }

    }

    public DataField finalizeAggregate() {
        return (DataField)sum;
    }

}
