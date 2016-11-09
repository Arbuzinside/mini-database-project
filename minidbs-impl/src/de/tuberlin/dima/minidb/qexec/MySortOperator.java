package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.qexec.heap.ExternalTupleSequenceIterator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeapException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by arbuzinside on 26.12.2015.
 */
public class MySortOperator implements SortOperator {



    private PhysicalPlanOperator child;
    private QueryHeap queryHeap;
    private int[] sortColumns;
    private int[] columnsAscending;
    private DataType[] columnTypes;
    private int estimatedCardinality;
    private DataTuple[] headList;
    private ExternalTupleSequenceIterator[] iterator;
    private TupleComparator comparator;
    private DataTuple[] sortArray;
    private int heapId;
    private int length;
    private int tupleSortAmount;
    private int tupleAmount;
    private int pointer;
    private boolean internal;

    
    

    public MySortOperator(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality, int[]  sortColumns, boolean[] columnsAscending) {

            this.child = child;
            this.queryHeap = queryHeap;
            this.sortColumns =  sortColumns;
            this.columnTypes = columnTypes;
            this.estimatedCardinality = estimatedCardinality;
            this.columnsAscending = new int[columnsAscending.length];

            for(int sortArray = 0; sortArray < columnsAscending.length; sortArray++) {
                this.columnsAscending[sortArray] = columnsAscending[sortArray] ? 1 : -1;
            }

         this.comparator = new TupleComparator(sortColumns, columnsAscending);


    }

    public void open(DataTuple correlatedTuple) throws QueryExecutionException {

            try {
                heapId = queryHeap.reserveSortHeap(columnTypes, estimatedCardinality);
                headList = queryHeap.getSortArray(heapId);
                tupleSortAmount = queryHeap.getMaximalTuplesForInternalSort(heapId);
            } catch (QueryHeapException ex) {
                throw new QueryExecutionException(ex.getMessage());
            }

            tupleAmount = 0;
            pointer = 0;
            length = 0;
            internal = false;
            child.open(correlatedTuple);

    }

    public DataTuple next() throws QueryExecutionException {
        DataTuple currentTuple;
        int position;
        if (internal) {
            try {
                if (length == 0) {
                    if (pointer < tupleAmount) {
                        currentTuple = headList[pointer];
                        pointer++;
                        return currentTuple;
                    } else {
                        return null;
                    }
                } else {
                    currentTuple = null;
                    position = -1;
                    for (int i = 0; i < length; ++i) {
                        DataTuple sortTuple = sortArray[i];
                        position = (currentTuple = currentTuple != null ? (sortTuple != null ?
                                (comparator.compare(currentTuple, sortTuple) <= 0 ? currentTuple : sortTuple)
                                : currentTuple)
                                : sortTuple) == sortTuple ? i : position;
                    }

                    if (currentTuple != null) {
                        sortArray[position] = iterator[position].hasNext() ? iterator[position].next() : null;
                    }
                    return currentTuple;
                }
            } catch (Exception ex) {
                throw new QueryExecutionException(ex.getMessage());
            }
        } else  {
            try {
                while ((currentTuple = child.next()) != null) {
                    if (tupleAmount == tupleSortAmount) {
                        Arrays.sort(headList, comparator);


                        try {
                            queryHeap.writeTupleSequencetoTemp(heapId, headList, tupleAmount);
                        } catch (IOException ex) {
                            throw new QueryExecutionException(ex.getMessage());
                        }

                        length++;
                        tupleAmount = 0;
                    }

                    headList[tupleAmount] = currentTuple;
                    tupleAmount++;
                }
                Arrays.sort(headList, comparator);
                if (length > 0) {
                    try {
                        queryHeap.writeTupleSequencetoTemp(heapId, headList, tupleAmount);
                    } catch (IOException ex) {
                        throw new QueryExecutionException(ex.getMessage());
                    }
                    length++;
                    tupleAmount = 0;
                    queryHeap.releaseSortArray(heapId);
                    headList = null;
                    iterator = queryHeap.getExternalSortedLists(heapId);

                    sortArray = new DataTuple[length];

                    for (position = 0; position < length; position++) {
                        if (iterator[position].hasNext()) {
                            sortArray[position] = iterator[position].next();
                        }
                    }
                }
            } catch (Exception ex) {
                queryHeap.releaseSortHeap(heapId);
            }

            internal = true;
            pointer = 0;
            return next();
        }
    }


    public void close() throws QueryExecutionException {
        headList = null;
        length = 0;
        tupleSortAmount = 0;
        tupleAmount = 0;
        pointer = 0;
        iterator = null;
        sortArray = null;
        if(heapId != 0) {
            queryHeap.releaseSortHeap(heapId);
            heapId = 0;
        }
        child.close();
    }

    private class TupleComparator implements Comparator<DataTuple> {

        private int[] sortColumns;
        private boolean[] columnsAscending;

        public TupleComparator(int[] sortColumns, boolean[] columnsAscending) {
            this.sortColumns = sortColumns;
            this.columnsAscending = columnsAscending;

        }

        @Override
        public int compare(DataTuple firstTuple, DataTuple secondTuple) {
            
            if(firstTuple == secondTuple)
                return 0;
            if(firstTuple == null)
                return 1;
            if(secondTuple == null)
                return -1;

            DataField firstField, secondField;
            for(int i = 0; i < sortColumns.length; i++) {

                firstField = firstTuple.getField(sortColumns[i]);
                secondField = secondTuple.getField(sortColumns[i]);

                if(firstField.compareTo(secondField) < 0)
                    return columnsAscending[i] ? -1 : 1;

                if(firstField.compareTo(secondField) > 0)
                    return columnsAscending[i] ? 1 : -1;
            }

            return 0;
        }
    }



}
