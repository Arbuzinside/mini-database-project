package de.tuberlin.dima.minidb.qexec.aggregators;

import de.tuberlin.dima.minidb.core.ArithmeticType;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class AggregatorAvg implements Aggregator {


    private DataType dataType;
    private ArithmeticType<DataField> sum;
    private ArithmeticType<DataField> avg;
    private int count;


    public AggregatorAvg(DataType dataType){

        this.dataType = dataType;
        sum = DataType.asArithmeticType(dataType.getNullValue());
    }



    public void initializeAggregate() {
        avg = sum.createZero();
        count = 0;
    }


    public void aggregateField(DataField dataField) {
        if(!dataField.isNULL()) {
            count++;
            avg.add(dataField);
        }

    }

    public DataField finalizeAggregate() {
        if(count == 0) {
            return dataType.getNullValue();
        } else {
            avg.divideBy(count);
            return (DataField)avg;
        }
    }



}
