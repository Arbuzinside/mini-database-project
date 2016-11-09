package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageCache;

/**
 * Created by arbuzinside on 16.11.2015.
 */
public class ReadOnce implements Runnable {


    private CacheableData data;
    private ReadRequest request;
    private PageCache cache;


    public ReadOnce(ReadRequest request, PageCache cache) {

        this.request = request;
        this.cache = cache;

    }


    public void run() {

        try {
            ResourceManager manager = request.getMng();
            data = manager.readPageFromResource(request.getBuffer(), request.getPageNumber());
            cache.addPage(data, request.getId());

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }

}
