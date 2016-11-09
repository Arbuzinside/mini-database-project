package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * Created by arbuzinside on 16.11.2015.
 */
public abstract class Request implements Comparable<Request> {

    protected int resourceId;
    protected int pageNumber;
    protected byte[] buffer;
    protected ResourceManager mng;
    protected boolean completed;
    protected CacheableData result;
    protected CacheableData data;


    public int getId() {
        return resourceId;
    }

    public void setId(int id) {
        this.resourceId = id;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }


    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public ResourceManager getMng() {
        return mng;
    }

    public void setMng(ResourceManager mng) {
        this.mng = mng;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public CacheableData getData() {
        return data;
    }

    public void setData(CacheableData data) {
        this.data = data;
    }

    public CacheableData getResult() {

        return result;
    }

    public void setResult(CacheableData result) {
        this.result = result;
    }

    public int compareTo(Request req) {
        if (pageNumber == req.pageNumber)
            return 0;
        return pageNumber < req.pageNumber ? -1 : 1;
    }
}
