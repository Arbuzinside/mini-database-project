package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * Created by arbuzinside on 16.11.2015.
 */
public class WriteRequest extends Request {


    private CacheableData data;

    public WriteRequest(int resourceId, int pageNumber, byte[] buffer, ResourceManager mng, CacheableData data) {

        this.resourceId = resourceId;
        this.pageNumber = pageNumber;
        this.mng = mng;
        this.buffer = buffer;
        this.setCompleted(false);
        this.setData(data);

    }


    public CacheableData getData() {
        return data;
    }

    public void setData(CacheableData data) {
        this.data = data;
    }


}