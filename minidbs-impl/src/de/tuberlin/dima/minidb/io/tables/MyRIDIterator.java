package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.util.Pair;

public class MyRIDIterator implements TupleRIDIterator {

    int position;
    int numCols;
    long columnBitmap;


    MyTablePage page;
    TableSchema schema;

    public MyRIDIterator(MyTablePage page, TableSchema schema, int numCols, long columnBitmap) {

        this.page = page;
        this.numCols = numCols;
        this.columnBitmap = columnBitmap;
        this.schema = schema;

        position = 0;

    }


    @Override
    public boolean hasNext() throws PageTupleAccessException {
        while (position < page.getNumRecordsOnPage() && page.isDead(position)) {
            position++;
        }
        return position < page.getNumRecordsOnPage();
    }

    @Override
    public Pair<DataTuple, RID> next() throws PageTupleAccessException {

        RID r = new RID((long) page.getPageNumber() << 32 | position & 0xFFFFFFFFL);
        DataTuple t = page.getDataTuple(position++, Long.MAX_VALUE, schema.getNumberOfColumns());

        return new Pair<DataTuple, RID>(t, r);
    }

}