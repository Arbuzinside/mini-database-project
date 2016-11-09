package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

/**
 * Created by arbuzinside on 30.10.2015.
 */
public class MyTablePage implements TablePage {


    private byte[] header = new byte[TABLE_DATA_PAGE_HEADER_BYTES];
    private byte[] page;

    private boolean isExpired;
    private boolean isModified;


    private byte[] pageNumber;
    private byte[] numberRecords;
    private byte[] recordWidth;
    private byte[] lengthChunk;


    private TableSchema schema;


    private byte[] pNumber;


    public MyTablePage(TableSchema schema, byte[] binaryPage) throws PageFormatException {


       if (schema.getPageSize().getNumberOfBytes() != binaryPage.length)
            throw new PageFormatException("size doesn't match");


        this.page = binaryPage;


        this.pNumber = MyHelper.intToBytes(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER);
        this.schema = schema;



        this.isModified = false;
        this.isExpired = false;


    }

    public MyTablePage(TableSchema schema, byte[] binaryPage, int pageNumber) throws PageFormatException {


       if (schema.getPageSize().getNumberOfBytes() != binaryPage.length)
           throw new PageFormatException("size doesn't match");



        this.page = binaryPage;



        this.pNumber = MyHelper.intToBytes(TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER);
        this.schema = schema;
        this.pageNumber = MyHelper.intToBytes(pageNumber);
        this.recordWidth = MyHelper.intToBytes(calcRecordWidth(schema));
        this.numberRecords = MyHelper.intToBytes(0);
        this.lengthChunk = MyHelper.intToBytes(schema.getPageSize().getNumberOfBytes());


        setHeader();


        this.isModified = true;
        this.isExpired = false;


    }


    /**
     * Function calculates the whole width of records
     *
     * @param schema
     * @return record Width Integer
     */
    private int calcRecordWidth(TableSchema schema) {

        //book 4 bytes for each metadata
        int rWidth = 4;

        for (int i = 0; i < schema.getNumberOfColumns(); i++) {
            DataType dataType = schema.getColumn(i).getDataType();

            if (dataType.isFixLength())
                rWidth += dataType.getNumberOfBytes();
            else
                rWidth += 8;

        }

        return rWidth;

    }


    private void setHeader() {

        page[0] = pNumber[0];   //magic number
        page[1] = pNumber[1];   //magic number
        page[2] = pNumber[2];     //magic number
        page[3] = pNumber[3];     //magic number
        page[4] = pageNumber[0];      //page number
        page[5] = pageNumber[1];      //page number
        page[6] = pageNumber[2];      //page number
        page[7] = pageNumber[3];      //page number
        page[8] = numberRecords[0];
        page[9] = numberRecords[1];
        page[10] = numberRecords[2];
        page[11] = numberRecords[3];
        page[12] = recordWidth[0];
        page[13] = recordWidth[1];
        page[14] = recordWidth[2];
        page[15] = recordWidth[3];
        page[16] = lengthChunk[0];
        page[17] = lengthChunk[1];
        page[18] = lengthChunk[2];
        page[19] = lengthChunk[3];
    }


    /**
     * Checks if this cacheable object is actually expired. An object is expired, when the
     * contents in the buffer behind that object is no longer in the cache or has been
     * overwritten.
     *
     * @return True, if the object is expired (no longer valid), false otherwise.
     */
    @Override
    public boolean isExpired() {

        return isExpired;
    }


    /**
     * Gets the binary buffer that contains the data that is wrapped by this page. All operations on this
     * page will affect the buffer that is returned by this method.
     *
     * @return The buffer containing the data wrapped by this page object.
     */
    public byte[] getBuffer() {

        return page;
    }


    /**
     * Marks this cached data object as invalid. This method should be called, when the
     * contents of the buffer behind the cached object is no longer in the cache, such that
     * this cacheable data object would now work on invalid data.
     */
    public void markExpired() {

        this.isExpired = true;
    }


    /**
     * This method checks, if the object has been modified since it was read from
     * the persistent storage. Objects that have been changed must be written back
     * before they can be evicted from the cache.
     *
     * @return true, if the data object has been modified, false if it is unchanged.
     * @throws PageExpiredException Thrown, if the operation is performed
     *                              on a page that is identified to be expired.
     */
    public boolean hasBeenModified() throws PageExpiredException {

        if (isExpired())
            throw new PageExpiredException();

        return isModified;
    }


    /**
     * Gets the page number of this page, as is found in the header bytes 4 - 7.
     *
     * @return The page number from the page header.
     * @throws PageExpiredException Thrown, if the operation is performed
     *                              on a page that is identified to be expired.
     */
    @Override
    public int getPageNumber() throws PageExpiredException {

        if (isExpired())
            throw new PageExpiredException();
        return MyHelper.byteArrayToInt(pageNumber);
    }


    /**
     * Gets how many records are currently stored on this page. This returns the total number
     * of records, including those that are marked as deleted. The number retrieved by this
     * function is the value from the header bytes 8 - 11.
     *
     * @return The total number of records on this page.
     * @throws PageExpiredException Thrown, if the operation is performed
     *                              on a page that is identified to be expired.
     */
    @Override
    public int getNumRecordsOnPage() throws PageExpiredException {

        if (isExpired())
            throw new PageExpiredException();

        return MyHelper.byteArrayToInt(page, 8);
    }

    /**
     * Gets record width, previously calculated in the constructor
     *
     * @return
     * @throws PageExpiredException
     */
    public int getRecordWidth() throws PageExpiredException {

        if (isExpired())
            throw new PageExpiredException();

        return MyHelper.byteArrayToInt(page, 12);

    }


    public int getChunkOffset() throws PageExpiredException {

        if (isExpired())
            throw new PageExpiredException();

        return MyHelper.byteArrayToInt(page, 16);
    }


    public int getTombstone(int position) throws PageExpiredException {

        if (isExpired)
            throw new PageExpiredException();

        int offset = TABLE_DATA_PAGE_HEADER_BYTES + position * getRecordWidth();

        return MyHelper.byteArrayToInt(page, offset);
    }


    // ------------------------------------------------------------------------

    /**
     * Inserts a tuple into the page by inserting the variable-length fields into the dedicated
     * part of the page and inserting the record for the tuple into the record sequence.
     * <p/>
     * If the method is not successful in inserting the tuple due to the fact that there is
     * not enough space left, it returns false, but does not throw an exception.
     *
     * @param tuple The tuple to be inserted.
     * @return true, if the tuple was inserted, false, if the tuple was not inserted.
     * @throws PageFormatException  Thrown, if the format of the page is invalid, such as that
     *                              current offset to the variable-length-chunk is invalid.
     * @throws PageExpiredException Thrown, if the operation is performed
     *                              on a page that is identified to be expired.
     */


    private int countTupleSize(DataTuple tuple) throws PageFormatException {

        int chunkWidth = 0;

        for (int i = 0; i < tuple.getNumberOfFields(); i++) {


            DataField field = tuple.getField(i);
            if (!field.getBasicType().equals(schema.getColumn(i).getDataType().getBasicType()))
                throw new PageFormatException("Tuple is incorrect");


            if (!field.getBasicType().isFixLength()) {
                chunkWidth += field.getNumberOfBytes();
            }

        }
        return chunkWidth;

    }


    @Override
    public boolean insertTuple(DataTuple tuple) throws PageFormatException, PageExpiredException {

        if (isExpired())
            throw new PageExpiredException();


        int chunkWidth = countTupleSize(tuple);
        int offset = header.length + getNumRecordsOnPage() * getRecordWidth();


        if (chunkWidth + getRecordWidth() > getChunkOffset() - offset)
            return false;

        page = MyHelper.intToBytes(page, 0, offset);

        int position = offset + 4; //metadata
        int chunk = getChunkOffset();

        for (int i = 0; i < tuple.getNumberOfFields(); i++) {


            DataField field = tuple.getField(i);


            if (field.getBasicType().isFixLength()) {

                field.encodeBinary(page, position);
                position += schema.getColumn(i).getDataType().getNumberOfBytes();

            } else {


                if (field.isNULL()) {
                    page = MyHelper.intToBytes(page, 0, position);
                    page = MyHelper.intToBytes(page, 0, position + 4);

                } else {
                    chunk -= field.getNumberOfBytes();


                    field.encodeBinary(page, chunk);

                    page = MyHelper.intToBytes(page, chunk, position);
                    page = MyHelper.intToBytes(page, field.getNumberOfBytes(), position + 4);

                }
                position += 8;
            }

        }

        page = MyHelper.intToBytes(page, getNumRecordsOnPage() + 1, 8);
        page = MyHelper.intToBytes(page, chunk, 16);


        this.isModified = true;

        return true;

    }


    /**
     * Deletes a tuple by setting the tombstone flag to 1.
     *
     * @param position The position of the tuple's record. The first record has position 0.
     * @throws PageTupleAccessException Thrown, if the index is negative or larger than the number
     *                                  of tuple on the page.
     * @throws PageExpiredException     Thrown, if the operation is performed
     *                                  on a page that is identified to be expired.
     */
    @Override
    public void deleteTuple(int position) throws PageExpiredException, PageTupleAccessException {

        if (isExpired())
            throw new PageExpiredException();
        if (position < 0 || position >= getNumRecordsOnPage())
            throw new PageTupleAccessException(position);

        int offset = MyHelper.byteArrayToInt(page, 12) * position + header.length;
        page = MyHelper.intToBytes(page, 1, offset);

        this.isModified = true;


    }


    public boolean isDead(int position) {
        return (getTombstone(position) & 0x1) == 1;
    }


    /**
     * Takes the DataTuple from the page whose record is found at the given position in the
     * sequence of records on the page. The position starts at 0, such that
     * <code>getDataTuple(0)</code> returns the tuple whose record starts directly after
     * the page header. The tuple contains all fields as describes in the table schema, that
     * means this function takes care of resolving the pointers into
     * actual variable-length fields.
     * <p/>
     * If the tombstone flag of the record is set, than this method returns null, but does
     * not throw an exception.
     *
     * @param position The position of the tuple's record. The first record has position 0.
     * @param bitmap   The bitmap describing which columns to fetch. See description of the class
     *                 for details on how the bitmaps describe which columns to fetch.
     * @param cols     The number of columns that should be fetched.
     * @return The tuple constructed from the record and its referenced variable-length fields,
     * or null, if the tombstone bit of the tuple is set.
     * @throws PageTupleAccessException Thrown, if the tuple could not be constructed (pointers invalid),
     *                                  or the index negative or larger than the number of tuple on the page.
     * @throws PageExpiredException     Thrown, if the operation is performed
     *                                  on a page that is identified to be expired.
     */
    @Override
    public DataTuple getDataTuple(int position, long bitmap, int cols) throws PageExpiredException, PageTupleAccessException {

        if (isExpired)
            throw new PageExpiredException();

        if (position < 0 || position > getNumRecordsOnPage())
            throw new PageTupleAccessException(position);


        int offset = getRecordWidth() * position + TABLE_DATA_PAGE_HEADER_BYTES;

        if ((MyHelper.byteArrayToInt(page, offset) & 0x1) == 1)
            return null;

        offset += 4;

        DataTuple tuple = new DataTuple(cols);
        DataType type;
        DataField field;
        int column = 0;

        for (int i = 0; i < schema.getNumberOfColumns(); i++) {

            type = schema.getColumn(i).getDataType();

            if ((bitmap & 0x1) == 0) {

                if (type.isFixLength())
                    offset += type.getNumberOfBytes();
                else
                    offset += 8;

            } else {
                if (type.isFixLength()) {
                    field = type.getFromBinary(page, offset);
                    offset += type.getNumberOfBytes();
                    if (field.getNumberOfBytes() != type.getNumberOfBytes()) {
                    }
                } else {
                    int start = MyHelper.byteArrayToInt(page, offset);
                    int length = MyHelper.byteArrayToInt(page, offset + 4);

                    if (start == 0 && length == 0)
                        field = type.getNullValue();
                    else
                        field = type.getFromBinary(page, start, length);
                    offset += 8;
                }
                tuple.assignDataField(field, column);
                column++;
            }
            bitmap >>= 1;
        }
        return tuple;
    }

    /**
     * Takes the DataTuple from the page whose record is found at the given position in the
     * sequence of records on the page. The position starts at 0, such that
     * <code>getDataTuple(0)</code> returns the tuple whose record starts directly after
     * the page header. The tuple is evaluated against the given predicates. If any of the
     * predicates evaluates to <i>false</i>, this method returns null.
     * <p/>
     * The tuple contains all fields as describes in the table schema, that means this function
     * takes care of resolving the pointers into actual variable-length fields.
     * <p/>
     * If the tombstone flag of the record is set, than this method returns null, but does
     * not throw an exception.
     *
     * @param preds    An array of predicates that the tuple must pass. The predicates are conjunctively
     *                 connected, so if any of the predicates evaluates to false, the tuple is discarded.
     * @param position The position of the tuple's record. The first record has position 0.
     * @param bitmap   The bitmap describing which columns to fetch. See description of the class
     *                 for details on how the bitmaps describe which columns to fetch.
     * @param cols     The number of columns that should be fetched.
     * @return The tuple constructed from the record and its referenced variable-length fields,
     * or null, if the tombstone bit of the tuple is set or any predicate evaluates to false.
     * @throws PageTupleAccessException Thrown, if the tuple could not be constructed (pointers invalid),
     *                                  or the index negative or larger than the number of tuple on the page.
     * @throws PageExpiredException     Thrown, if the operation is performed
     *                                  on a page that is identified to be expired.
     */
    @Override
    public DataTuple getDataTuple(LowLevelPredicate[] preds, int position, long bitmap, int cols) throws PageTupleAccessException,
            PageExpiredException {

        if (isExpired)
            throw new PageExpiredException();

        if (position < 0 || position > getNumRecordsOnPage())
            throw new PageTupleAccessException(position);

        int offset = getRecordWidth() * position + header.length;

        if ((MyHelper.byteArrayToInt(page, offset) & 0x1) == 1)
            return null;


        offset += 4;

        DataTuple tuple = new DataTuple(cols);
        DataType type;
        DataField field;
        int column = 0;


        for (int i = 0; i < schema.getNumberOfColumns(); i++) {
            type = schema.getColumn(i).getDataType();

            if (type.isFixLength()) {
                field = type.getFromBinary(page, offset);
                offset += type.getNumberOfBytes();
            } else {
                int start = MyHelper.byteArrayToInt(page, offset);
                int length = MyHelper.byteArrayToInt(page, offset + 4);

                if (start == 0 && length == 0) {
                    field = type.getNullValue();
                } else {

                    field = type.getFromBinary(page, start, length);
                }
                offset += 8;
            }


            for (int j = 0; j < preds.length; j++) {
                if (preds[j].getColumnIndex() == i) {
                    if (!preds[j].evaluateWithNull(field))
                        return null;
                }
            }

            if ((bitmap & 0x1) == 1) {
                tuple.assignDataField(field, column);
                column++;
            }

            bitmap >>= 1;
        }

        return tuple;

    }

    /**
     * Creates an iterator that iterates over all tuples contained in this page. Records whose tombstone
     * bit is set are skipped.
     *
     * @param cols   The number of columns that should be fetched.
     * @param bitmap The bitmap describing which columns to fetch. See description of the class
     *               for details on how the bitmaps describe which columns to fetch.
     * @return An iterator over the tuples represented by the records in this page.
     * @throws PageTupleAccessException Thrown, if the iterator could not be created due to
     *                                  invalid format.
     * @throws PageExpiredException     Thrown, if the operation is performed
     *                                  on a page that is identified to be expired.
     */
    @Override
    public TupleIterator getIterator(int cols, long bitmap) throws PageTupleAccessException, PageExpiredException {

        if (isExpired) throw new PageExpiredException();


        return new MyIterator(this, cols, bitmap);
    }

    /**
     * Creates an iterator that iterates over all tuple contained in this page. This means
     * that tuples, where the record has been marked as deleted (i.e. whose tombstone bit is set)
     * are skipped. Only tuples that pass all predicates are returned.
     *
     * @param preds  An array of predicates that the tuple must pass. The predicates are conjunctively
     *               connected, so if any of the predicates evaluates to false, the tuple is discarded.
     * @param cols   The number of columns that should be fetched.
     * @param bitmap The bitmap describing which columns to fetch. See description of the class
     *               for details on how the bitmaps describe which columns to fetch.
     * @return An iterator over the tuples represented by the records in this page.
     * @throws PageTupleAccessException Thrown, if the iterator could not be created due to
     *                                  invalid format.
     * @throws PageExpiredException     Thrown, if the operation is performed
     *                                  on a page that is identified to be expired.
     */
    @Override
    public TupleIterator getIterator(LowLevelPredicate[] preds, int cols, long bitmap) {

        if (isExpired) throw new PageExpiredException();

        return new MyIterator(this, preds, cols, bitmap);
    }

    /**
     * Creates an iterator as the function <code>getIterator()</code> does. In addition to the tuples,
     * this iterator the RID that referenced the tuple's record.
     *
     * @return An iterator over the tuple sequence, where the tuples contain in addition the RID.
     * @throws PageTupleAccessException Thrown, if the iterator could not be created due to
     *                                  invalid format.
     * @throws PageExpiredException     Thrown, if the operation is performed
     *                                  on a page that is identified to be expired.
     */
    @Override
    public TupleRIDIterator getIteratorWithRID()
            throws PageTupleAccessException, PageExpiredException {

        if (isExpired())
            throw new PageExpiredException("Page " + pageNumber + "is expired");

        int cols = schema.getNumberOfColumns();

        return new MyRIDIterator(this, this.schema, cols, Long.MAX_VALUE);

    }


}
