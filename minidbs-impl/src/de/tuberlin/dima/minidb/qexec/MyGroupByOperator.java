package de.tuberlin.dima.minidb.qexec;

/**
 * Created by arbuzinside on 27.12.2015.
 */
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.qexec.aggregators.*;


public class MyGroupByOperator implements GroupByOperator {
    private PhysicalPlanOperator child;
    private int[] groupColumnIndices;
    private int[] aggColumnIndices;
    private int[] groupColumnOutputPositions;
    private int[] aggregateColumnOutputPosition;
    private DataField[] result;
    private Aggregator[] aggregator;
    private boolean isNext = true;




    public MyGroupByOperator(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices, AggregationType[] aggregateFunctions,
                             DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {


            this.child = child;
            this.groupColumnIndices = groupColumnIndices;
            this.aggColumnIndices = aggColumnIndices;
            this.groupColumnOutputPositions = groupColumnOutputPositions;
            this.aggregateColumnOutputPosition = aggregateColumnOutputPosition;
            this.result = new DataField[groupColumnIndices.length];
            this.aggregator = new Aggregator[aggregateFunctions.length];

            for(int i = 0; i < this.aggregator.length; i++) {
                switch(aggregateFunctions[i].ordinal()) {
                    case 1:
                        this.aggregator[i] = new AggregatorCount();
                        break;
                    case 2:
                        this.aggregator[i] = new AggregatorSum(aggColumnTypes[i]);
                        break;
                    case 3:
                        this.aggregator[i] = new AggregatorAvg(aggColumnTypes[i]);
                        break;
                    case 4:
                        this.aggregator[i] = new AggregatorMin(aggColumnTypes[i]);
                        break;
                    case 5:
                        this.aggregator[i] = new AggregatorMax(aggColumnTypes[i]);
                        break;
                }
            }


    }

    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        int i;
        for(i = 0; i < this.result.length; i++) {
            this.result[i] = null;
        }

        for(i = 0; i < this.aggregator.length; i++) {
            this.aggregator[i].initializeAggregate();
        }

        this.isNext = false;
        this.child.open(correlatedTuple);
    }

    public DataTuple next() throws QueryExecutionException {


            DataTuple currentTuple;
            while((currentTuple = child.next()) != null) {
                boolean isSameGroup = true;

                int i;
                for(i = 0; i < this.groupColumnIndices.length; ++i) {
                    if(!currentTuple.getField(this.groupColumnIndices[i]).equals(this.result[i])) {
                        isSameGroup = false;
                        break;
                    }
                }

                if(isSameGroup) {
                    for(i = 0; i < this.aggColumnIndices.length; ++i)
                        this.aggregator[i].aggregateField(currentTuple.getField(this.aggColumnIndices[i]));
                } else {
                    if (this.result[0] != null) {
                        DataTuple aggTuple = this.formatTuple();
                        this.formatTuple(currentTuple);
                        return aggTuple;
                    } else {

                        this.formatTuple(currentTuple);
                    }
                }
            }

            if(this.isNext) return null;

            this.isNext = true;
            if(!(this.result.length <= 0 || this.result[0] != null)) {
                return null;
            }

            return this.formatTuple();

    }

    public void close() throws QueryExecutionException {
        child.close();
    }

    private void formatTuple(DataTuple tuple) {

        for(int i = 0; i < this.groupColumnIndices.length; ++i) {
            this.result[i] = tuple.getField(this.groupColumnIndices[i]);
        }

        for( int i = 0; i < this.aggregator.length; ++i) {
            this.aggregator[i].initializeAggregate();
            this.aggregator[i].aggregateField(tuple.getField(this.aggColumnIndices[i]));
        }

    }

    private DataTuple formatTuple() {


        DataTuple tuple = new DataTuple(this.groupColumnOutputPositions.length);

        int value;
        for(int i = 0; i < this.groupColumnOutputPositions.length; ++i) {
            if((value = this.groupColumnOutputPositions[i]) != -1) {
                tuple.assignDataField(this.result[value], i);
            }
        }

        for(int i = 0; i < this.aggregateColumnOutputPosition.length; ++i) {
            if((value = this.aggregateColumnOutputPosition[i]) != -1) {
                tuple.assignDataField(this.aggregator[value].finalizeAggregate(), i);
            }
        }

        return tuple;
    }
}
