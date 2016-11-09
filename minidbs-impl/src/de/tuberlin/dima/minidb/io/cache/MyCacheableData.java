package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.tables.MyHelper;
import de.tuberlin.dima.minidb.io.tables.MyTablePage;

/**
 * Created by arbuzinside on 3.11.2015.
 */
public class MyCacheableData implements CacheableData {


    private CacheableData page;
    private byte[] buffer;
    private int hits;
    private int pinCounter;
    private int id;
    private boolean isDeleted;

    private boolean isExpired;



    public MyCacheableData(CacheableData page, int id){

        this.page = page;
        this.buffer = page.getBuffer();
        hits = 0;
        pinCounter = 0;
        this.id = id;
        isExpired = false;
        isDeleted = false;
    }


    /**
     * This method checks, if the object has been modified since it was read from
     * the persistent storage. Objects that have been changed must be written back
     * before they can be evicted from the cache.
     *
     * @return true, if the data object has been modified, false if it is unchanged.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    public boolean hasBeenModified() throws PageExpiredException{

        return true;
    }

    /**
     * Gets the number of the page that is represented by this object.
     *
     * @return The page number.
     * @throws PageExpiredException Thrown, if the operation is performed
     * 								on a page that is identified to be expired.
     */
    public int getPageNumber() throws PageExpiredException{

        if (isExpired())
            throw new PageExpiredException();
        return MyHelper.byteArrayToInt(page.getBuffer());
    }

    /**
     * Marks this cached data object as invalid. This method should be called, when the
     * contents of the buffer behind the cached object is no longer in the cache, such that
     * this cacheable data object would now work on invalid data.
     */
    public void markExpired(){

    }

    /**
     * Checks if this cacheable object is actually expired. An object is expired, when the
     * contents in the buffer behind that object is no longer in the cache or has been
     * overwritten.
     *
     * @return True, if the object is expired (no longer valid), false otherwise.
     */
    public boolean isExpired(){

        return isExpired;
    }

    /**
     * Gets the binary buffer that contains the data that is wrapped by this page. All operations on this
     * page will affect the buffer that is returned by this method.
     *
     * @return The buffer containing the data wrapped by this page object.
     */
    public byte[] getBuffer(){

        return this.buffer;
    }


    public CacheableData getPage(){

        return this.page;
    }




    public void hitPage(){

        hits++;
    }

    public int getHits(){

        return hits;
    }

    public int isPinned(){

        return pinCounter;
    }

    public int pinPage(){

        pinCounter++;
        return pinCounter;
    }

    public int unpinPage(){

        pinCounter--;

        return pinCounter;
    }

    public void setPinToZero(){

        pinCounter = 0;

    }


    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }
}
