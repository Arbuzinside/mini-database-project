package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by arbuzinside on 16.12.2015.
 */
public class MyFetchOperator implements FetchOperator {


    private PhysicalPlanOperator child;
    private BufferPoolManager bufferPool;
    private int tableResourceId;
    private int[] outputColumnMap;
    private long bitmap;

    private ArrayList<Integer> columnList;


    public MyFetchOperator(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {

        this.child = child;
        this.bufferPool = bufferPool;
        this.tableResourceId = tableResourceId;
        this.outputColumnMap = outputColumnMap;

        Set<Integer> columns = new HashSet<>();

        for (int i : outputColumnMap) {
            columns.add(i);
        }

        this.columnList = new ArrayList(columns);
        Collections.sort(columnList);


        for (int i : columnList) {
            this.bitmap += Math.pow(2, i);
        }

    }

    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

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
    public DataTuple next() throws QueryExecutionException {

        DataTuple tuple = child.next();

        if (tuple != null) {

            DataTuple out;
            TablePage currentPage;
            RID currentRID = (RID) tuple.getField(0);
            try {
                currentPage = (TablePage) bufferPool.getPageAndPin(tableResourceId, currentRID.getPageIndex());
                out = formatTuple(currentPage.getDataTuple(currentRID.getTupleIndex(), bitmap, columnList.size()));
                bufferPool.unpinPage(tableResourceId, currentRID.getPageIndex());
                return out;

            } catch (Exception ex) {
                throw new QueryExecutionException();
            }

        }


        return null;
    }


    private DataTuple formatTuple(DataTuple tuple) {

        DataTuple newTuple = new DataTuple(outputColumnMap.length);
        for (int i = 0; i < columnList.size(); i++) {
            for (int j = 0; j < outputColumnMap.length; j++) {
                if (outputColumnMap[j] == columnList.get(i))
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

        child.close();
    }
}
