package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * Created by arbuzinside on 14.11.2015.
 */
public class ReadRequest extends Request {


    private CacheableData result;

    public ReadRequest(int resourceId, int pageNumber, byte[] buffer, ResourceManager mng) {

        this.resourceId = resourceId;
        this.pageNumber = pageNumber;
        this.mng = mng;
        this.buffer = buffer;
        this.setCompleted(false);

    }


    public CacheableData getResult() {

        return result;
    }

    public void setResult(CacheableData result) {
        this.result = result;
    }
}
