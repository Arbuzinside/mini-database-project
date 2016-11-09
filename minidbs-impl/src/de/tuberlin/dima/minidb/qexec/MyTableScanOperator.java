package de.tuberlin.dima.minidb.qexec;


import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by arbuzinside on 8.12.2015.
 */
public class MyTableScanOperator implements TableScanOperator {


    int prefetchWindowLength;
    private BufferPoolManager bufferPool;
    private TableResourceManager manager;
    private int resourceId;
    private int[] producedColumnIndexes;
    private LowLevelPredicate[] predicate;
    private long bitmap;


    private int currentPageNumber;
    private ArrayList<Integer> columnList;

    private TupleIterator tupleIterator;


    public MyTableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
                               int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) {

        this.bufferPool = bufferPool;
        this.manager = tableManager;
        this.resourceId = resourceId;
        this.producedColumnIndexes = producedColumnIndexes;
        this.predicate = predicate;
        this.prefetchWindowLength = prefetchWindowLength;
        this.currentPageNumber = manager.getFirstDataPageNumber();
        Set<Integer> columns = new HashSet<>();

        for (int i : producedColumnIndexes) {
            columns.add(i);
        }

        this.columnList = new ArrayList(columns);
        Collections.sort(columnList);

        //2,4 -> 10100

        for (int i : columnList) {
            this.bitmap += Math.pow(2, i);
        }


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


        TablePage page;

        try {

            bufferPool.prefetchPages(resourceId, currentPageNumber, manager.getLastDataPageNumber());
            page = (TablePage) bufferPool.getPageAndPin(resourceId, currentPageNumber);
            tupleIterator = page.getIterator(predicate, columnList.size(), bitmap);
        } catch (BufferPoolException ex) {
            throw new QueryExecutionException();
        } catch (IOException ex) {
            throw new QueryExecutionException();
        } catch (PageTupleAccessException ex) {
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
        try {
            if (tupleIterator.hasNext())
                return formatTuple(tupleIterator.next());
            else {
                while (currentPageNumber < manager.getLastDataPageNumber()) {
                    bufferPool.unpinPage(resourceId, currentPageNumber);
                    currentPageNumber++;
                    try {
                        TablePage page = (TablePage) bufferPool.getPageAndPin(resourceId, currentPageNumber);
                        tupleIterator = page.getIterator(predicate, columnList.size(), bitmap);
                        if (tupleIterator.hasNext())
                            return formatTuple(tupleIterator.next());
                    } catch (BufferPoolException ex) {
                        throw new QueryExecutionException();
                    } catch (IOException ex) {
                        throw new QueryExecutionException();
                    } catch (PageTupleAccessException ex) {
                        throw new QueryExecutionException();
                    }
                }
            }
        } catch (PageTupleAccessException ex) {
            throw new QueryExecutionException();
        }
        return null;
    }

    private DataTuple formatTuple(DataTuple tuple) {
        DataTuple newTuple = new DataTuple(producedColumnIndexes.length);
        for (int i = 0; i < columnList.size(); i++) {
            for (int j = 0; j < producedColumnIndexes.length; j++) {
                if (producedColumnIndexes[j] == columnList.get(i))
                    newTuple.assignDataField(tuple.getField(i), j);
            }
        }
        return newTuple;
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
