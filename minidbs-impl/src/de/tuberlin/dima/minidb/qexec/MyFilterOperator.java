package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;

/**
 * Created by arbuzinside on 16.12.2015.
 */
public class MyFilterOperator implements FilterOperator {


    private PhysicalPlanOperator child;
    private LocalPredicate predicate;


    public MyFilterOperator(PhysicalPlanOperator child, LocalPredicate predicate){

        this.child = child;
        this.setPredicate(predicate);

    }


    public void open(DataTuple correlatedTuple) throws QueryExecutionException{

        child.open(correlatedTuple);

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


        DataTuple tuple = child.next();

        while (tuple != null){
            if (getPredicate().evaluate(tuple))
                return tuple;

            tuple = child.next();
        }

        return null;
    }


    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    public void close() throws QueryExecutionException{


    }


    @Override
    public LocalPredicate getPredicate() {
        return predicate;
    }

    public void setPredicate(LocalPredicate predicate) {
        this.predicate = predicate;
    }
}
