package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

/**
 * Created by arbuzinside on 2.11.2015.
 */
public class MyIterator implements TupleIterator {

    int position;
    int numCols;
    long columnBitmap;
    LowLevelPredicate[] preds;

    MyTablePage page;

    public MyIterator(MyTablePage page, int numCols, long columnBitmap) {

        this.page = page;
        this.numCols = numCols;
        this.columnBitmap = columnBitmap;

        position = 0;

    }

    public MyIterator(MyTablePage page, LowLevelPredicate[] preds, int numCols, long columnBitmap) {

        this.page = page;
        this.numCols = numCols;
        this.columnBitmap = columnBitmap;
        this.preds = preds;
        position = 0;

    }

    @Override
    public boolean hasNext() throws PageTupleAccessException {
        boolean skip = true;

        if (preds != null) {


            while (position < page.getNumRecordsOnPage() && skip) {
                if (page.isDead(position) || page.getDataTuple(preds, position, columnBitmap, numCols) == null) {
                    position++;
                } else {
                    skip = false;
                }
            }
            return position < page.getNumRecordsOnPage() && page.getDataTuple(preds, position, columnBitmap, numCols) != null;
        } else {

            while (position < page.getNumRecordsOnPage() && skip) {
                if (page.isDead(position) || page.getDataTuple(position, columnBitmap, numCols) == null) {
                    position++;
                } else {
                    skip = false;
                }
            }
            return position < page.getNumRecordsOnPage() && page.getDataTuple(position, columnBitmap, numCols) != null;
        }
    }


    @Override
    public DataTuple next() throws PageTupleAccessException {

        if (preds != null)
            return page.getDataTuple(preds, position++, columnBitmap, numCols);
        else
            return page.getDataTuple(position++, columnBitmap, numCols);

    }

}
