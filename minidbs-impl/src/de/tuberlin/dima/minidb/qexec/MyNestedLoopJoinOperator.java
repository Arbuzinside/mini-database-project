package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class MyNestedLoopJoinOperator implements NestedLoopJoinOperator {


    private PhysicalPlanOperator outerChild;
    private PhysicalPlanOperator innerChild;
    private JoinPredicate joinPredicate;
    private int[] columnMapOuterTuple;
    private int[] columnMapInnerTuple;

    private DataTuple outerTuple;



    public MyNestedLoopJoinOperator(PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild, JoinPredicate joinPredicate,
                                    int[] columnMapOuterTuple, int[] columnMapInnerTuple){

        this.outerChild = outerChild;
        this.innerChild = innerChild;
        this.joinPredicate = joinPredicate;
        this.columnMapInnerTuple = columnMapInnerTuple;
        this.columnMapOuterTuple = columnMapOuterTuple;

    }



    public void open(DataTuple correlatedTuple) throws QueryExecutionException{

        outerChild.open(correlatedTuple);
        outerTuple = outerChild.next();
        innerChild.open(outerTuple);

    }


    /**
     * Produces the next batch of tuples from the plan below and stores them in the
     * given array. Returns true if a full batch was returned, false otherwise.
     *
     * @return The next tuple, produced by this operator.
     * @throws QueryExecutionException Thrown, if the query execution could not be completed
     *                                 for whatever reason.
     */
    public DataTuple next() throws QueryExecutionException{


        DataTuple innerTuple;

        /**
        * // S is the outer child of the Nested-Loop-Join.
        * // R is the inner child of the Nested-Loop-Join.
        *
        * next() {
            *   do {
                *     r := R.next();
                *     if (r == null) { // not found, R is exhausted for the current s
                    *       R.close();
                    *       s := S.next();
                    *       if (s == null) { // not found, both R and S are exhausted
                        *         return null;
                        *       }
                    *       R.open(s);    // open the plan correlated to the outer tuple
                    *       r := R.next();
                    *     }
                *   } while ( r.keyColumn != s.keyColumn ) // until predicate is fulfilled
            *
            *   return concatenation of r and s;
            * }
            */


        do {

            while((innerTuple = innerChild.next()) == null) {

                innerChild.close();

                outerTuple = outerChild.next();

                if (outerTuple == null) return null;

                innerChild.open(outerTuple);

            }

        } while (joinPredicate != null && !joinPredicate.evaluate(outerTuple, innerTuple));


        return formatTuple(outerTuple, innerTuple);

    }


    private DataTuple formatTuple(DataTuple outer, DataTuple inner){


        int size = columnMapOuterTuple.length;
        DataTuple result = new DataTuple(size);

        for (int i = 0; i < size; i++) {
            if(columnMapOuterTuple[i] != -1)
                result.assignDataField(outer.getField(columnMapOuterTuple[i]), i);
            else if (columnMapInnerTuple[i] != -1)
                result.assignDataField(inner.getField(columnMapInnerTuple[i]), i);
        }

        return result;
    }


    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    public void close() throws QueryExecutionException{

        innerChild.close();
        outerChild.close();

    }


    /**
     * Gets the operator rooting the outer sub-plan.
     *
     * @return The outer child operator.
     */
    public PhysicalPlanOperator getOuterChild(){

        return outerChild;
    }

    /**
     * Gets the operator rooting the inner sub-plan.
     *
     * @return The inner child operator.
     */
    public PhysicalPlanOperator getInnerChild(){

        return innerChild;
    }

    /**
     * Gets the join predicate applied in this join.
     *
     * @return This joins join predicate.
     */
    public JoinPredicate getJoinPredicate(){

        return joinPredicate;
    }
}
