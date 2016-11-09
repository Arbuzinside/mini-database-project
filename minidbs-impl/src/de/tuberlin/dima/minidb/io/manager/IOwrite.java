package de.tuberlin.dima.minidb.io.manager;


import de.tuberlin.dima.minidb.io.cache.CacheableData;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by arbuzinside on 11.11.2015.
 */
public class IOwrite implements Runnable {


    private Queue<Request> queue;
    private volatile boolean running;

    public IOwrite() {

        this.setQueue(new LinkedBlockingQueue<Request>());
        this.running = true;

    }

    public void run() {

        Request request;
        while (running) {
            request = getQueue().peek();

            if (request != null) {
                synchronized (request) {
                    try {
                        ResourceManager manager = request.getMng();

                        synchronized (manager) {
                            manager.writePageToResource(request.getBuffer(), request.getData());
                        }
                        getQueue().remove(request);
                        request.setCompleted(true);
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage() + request.getId());
                    } finally {
                        request.notifyAll();
                    }
                }
            }
        }
    }


    public void stopThread() {

        running = false;


    }

    public void addRequest(Request request) {
        this.getQueue().add(request);
    }


    public Queue<Request> getQueue() {
        return queue;
    }

    public void setQueue(LinkedBlockingQueue<Request> queue) {
        this.queue = queue;
    }


    public CacheableData getRequest(int id, int pageNumber) {

        CacheableData data = null;

        for (Request req : queue) {
            if (req.getId() == id && req.getPageNumber() == pageNumber)
                data = req.getData();

        }
        return data;
    }
}