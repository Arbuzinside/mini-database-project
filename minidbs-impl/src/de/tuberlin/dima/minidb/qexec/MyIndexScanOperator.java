package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

/**
 * Created by arbuzinside on 8.12.2015.
 */
public class MyIndexScanOperator implements IndexScanOperator {


    boolean startKeyIncluded;
    boolean stopKeyIncluded;
    private BTreeIndex index;
    private DataField startKey;
    private DataField stopKey;
    private IndexResultIterator indexIterator;


    public MyIndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {

        this.index = index;
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.startKeyIncluded = startKeyIncluded;
        this.stopKeyIncluded = stopKeyIncluded;

    }



    /**
     * Opens the plan below below and including this operator. Sets the initial status
     * necessary to start executing the plan.
     * <p/>
     * In the case that the tuples produced by the plan rooted at this operator
     * are correlated to another stream of tuples, the plan is opened and closed for
     * every such. The OPEN call gets as parameter the tuple for which the correlated
     * tuples are to be produced this time.
     * The most common example for that is the inner side of a nested-loop-join,
     * which produces tuples correlated to the current tuple from the outer side
     * (outer loop).
     * <p/>
     * Consider the following pseudo code, describing the next() function of a
     * nested loop join:
     * <p/>
     * // S is the outer child of the Nested-Loop-Join.
     * // R is the inner child of the Nested-Loop-Join.
     * <p/>
     * next() {
     * do {
     * r := R.next();
     * if (r == null) { // not found, R is exhausted for the current s
     * R.close();
     * s := S.next();
     * if (s == null) { // not found, both R and S are exhausted
     * return null;
     * }
     * R.open(s);    // open the plan correlated to the outer tuple
     * r := R.next();
     * }
     * } while ( r.keyColumn != s.keyColumn ) // until predicate is fulfilled
     * <p/>
     * return concatenation of r and s;
     * }
     *
     * @param epoch           The epoch the tuples to be returned have to belong to.
     * @param correlatedTuple The tuple for which the correlated tuples in the sub-plan
     *                        below and including this operator are to be fetched. Is
     *                        null is the case that the plan produces no correlated tuples.
     * @throws QueryExecutionException Thrown, if the operator could not be opened,
     *                                 that means that the necessary actions failed.
     */
    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

        try {
            indexIterator = index.lookupKeys(startKey, stopKey, startKeyIncluded, stopKeyIncluded);
        } catch (IOException ex) {
            throw new QueryExecutionException();
        } catch (PageFormatException ex) {
            throw new QueryExecutionException();
        }

    }


    /**
     * Produces the next batch of tuples from the plan below and stores them in the
     * given array. Returns true if a full batch was returned, false otherwise.
     *
     * @return The next tuple, produced by this operator.
     * @throws QueryExecutionException Thrown, if the query execution could not be completed
     *                                 for whatever reason.
     */
    public DataTuple next() throws QueryExecutionException {

        DataTuple tuple = new DataTuple(1);
        try {
            if (indexIterator.hasNext()) {
                DataField dataField = (DataField) indexIterator.next();
                tuple.assignDataField(dataField, 0);
                return tuple;
            }
        } catch (PageFormatException ex) {
            throw new QueryExecutionException();
        } catch (IOException ex) {
            throw new QueryExecutionException();
        }
        return null;
    }


    /**
     * Closes the query plan by releasing all resources and runtime structures used
     * in this operator and the plan below.
     *
     * @throws QueryExecutionException If the cleanup operation failed.
     */
    public void close() throws QueryExecutionException {

    }

}
