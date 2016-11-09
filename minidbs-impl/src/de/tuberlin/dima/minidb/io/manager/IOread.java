package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by arbuzinside on 11.11.2015.
 */
public class IOread implements Runnable {


    public ConcurrentLinkedQueue<Request> queue;
    private CacheableData data;
    private volatile boolean running;

    public IOread() {

        this.setQueue(new ConcurrentLinkedQueue<Request>());
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
                            data = manager.readPageFromResource(request.getBuffer(), request.getPageNumber());
                        }
                        request.setResult(data);
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


    public void addRequest(Request request) {

        this.getQueue().add(request);

    }

    public CacheableData getRequest(int id, int pageNumber) {

        CacheableData result = null;
        for (Request req : queue) {
            if (req.getId() == id && req.getPageNumber() == pageNumber) {
                try {
                    synchronized (req) {
                        while (!req.isCompleted()) {
                            req.wait();
                        }
                        result = req.getResult();
                    }
                } catch (InterruptedException ex) {
                    System.out.println(ex.getMessage());
                }
            }

        }
        return result;
    }


    public void stopThread() {
        running = false;

    }


    public Queue<Request> getQueue() {
        return queue;
    }

    public void setQueue(ConcurrentLinkedQueue<Request> queue) {
        this.queue = queue;
    }
}
