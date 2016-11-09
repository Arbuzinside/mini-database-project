package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.index.IndexResultIterator;

import java.io.IOException;

/**
 * Created by arbuzinside on 16.12.2015.
 */
public class MyIndexLookupOperator implements IndexLookupOperator {



    private BTreeIndex index;
    private DataField equalityLiteral;

    private IndexResultIterator<RID> indexIterator;

   public MyIndexLookupOperator(BTreeIndex index, DataField equalityLiteral){

       this.equalityLiteral = equalityLiteral;
       this.index = index;

   }

    public void open(DataTuple correlatedTuple) throws QueryExecutionException{

        try {
            indexIterator = index.lookupRids(equalityLiteral);
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
    public DataTuple next() throws QueryExecutionException{

        DataTuple tuple = new DataTuple(1);
        try {
            if (indexIterator.hasNext()) {
                DataField dataField = indexIterator.next();
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
    public void close() throws QueryExecutionException{

        indexIterator = null;

    }


}
