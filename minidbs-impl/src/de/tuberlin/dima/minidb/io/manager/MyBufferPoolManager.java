package de.tuberlin.dima.minidb.io.manager;


import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.io.cache.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import java.util.logging.Logger;


/**
 * Class used to provide uniform and thread safe access to pages. Pages are either fetched from the cache,
 * or, if not found in the cache, loaded from secondary storage. All methods in this class must be designed
 * for concurrent access by multiple threads. Any modification to shared structures must be synchronized or
 * realized through other atomic operations to prevent inconsistencies.
 * <p/>
 * <p/>
 * <p/>
 * General mechanism of synchronization:
 * <ul>
 * <li>All accesses to one of the caches are synchronized on that specific cache object.</li>
 * <li>All accesses to the load and write queue are synchronized through a central lock or monitor.
 * <li>When adding an item that was not found in the cache to the queues, the cache lock
 * must not be released in the meantime to make sure that a cache miss and the addition
 * to the queues remains atomic.</li>
 * <li>When removing pages from the queues and adding them to the cache, the operation must
 * hold both the cache and the monitor lock before starting to remove the pages.</li>
 * </ul>
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MyBufferPoolManager implements BufferPoolManager {
    /**
     * The maximum number of requests that may be queued together. If multiple requests on the same
     * resource are found together, they are queued together and handled together in an "elevator" like
     * fashion.
     */
    public static final int MAX_PAGE_REQUESTS_IN_SINGLE_QUEUE = 32;

    private Config config;
    private Logger logger;

    private int numberIOBuffers;

    private IOread readThread;
    private IOwrite writeThread;

    private boolean isOpened;

    private HashMap<PageSize, PageCache> caches;
    private HashMap<PageSize, Vector<byte[]>> buffers;
    private HashMap<Integer, ResourceManager> resourceManagers;

    private Thread w;
    private Thread r;


    public MyBufferPoolManager(Config config, Logger logger) {
        this.config = config;
        this.logger = logger;


        this.resourceManagers = new HashMap<>();
        numberIOBuffers = config.getNumIOBuffers();

        caches = new HashMap<>();
        buffers = new HashMap<>();

        readThread = new IOread();
        writeThread = new IOwrite();

        this.isOpened = true;


    }


    /**
     * Starts the operation of the I/O threads in the buffer pool.
     */
    public void startIOThreads() throws BufferPoolException {


        r = new Thread(readThread);
        w = new Thread(writeThread);

        r.start();
        w.start();

    }

    /**
     * This method closes the buffer pool. It causes the following actions:
     * <ol>
     * <li>It prevents further requests from being processed</li>
     * <li>Its stops the reading thread from fetching any further pages. Requests that are currently in the
     * queues to be read are discarded.</li>
     * <li>All methods that are currently blocked and waiting on a synchronous page request will be waken up and
     * must throw a BufferPoolException as soon as they wake up.</li>
     * <li>It closes all resources, meaning that it gets all their pages from the cache, and queues the modified
     * pages to be written out.</li>
     * <li>It dereferences the caches (setting them to null), allowing the garbage collection to
     * reclaim the memory.</li>
     * </ol>
     */
    public void closeBufferPool() {

        readThread.stopThread();
        writeThread.stopThread();
        r.interrupt();
        w.interrupt();
        isOpened = false;
    }

    /**
     * Registers a resource with this buffer pool. All requests for this resource are then served through this buffer pool.
     * <p/>
     * If the buffer pool has already a cache for the page size used by that resource, the cache will
     * be used for the resource. Otherwise, a new cache will be created for that page size. The size
     * of the cache that is to be created is read from the <tt>Config</tt> object. In addition to the
     * cache, the buffer pool must also create the I/O buffers for that page size.
     *
     * @param id      The id of the resource.
     * @param manager The ResourceManager through which pages of the resource are read from and
     *                written to secondary storage.
     * @throws BufferPoolException Thrown, if the buffer pool has been closed, the resource is already
     *                             registered with another handler or a cache needs to be created but
     *                             the creation fails.
     */
    public void registerResource(int id, ResourceManager manager)
            throws BufferPoolException {


        if (!isOpened)
            throw new BufferPoolException("Listener is shut down");

        if (resourceManagers.containsKey(id))
            throw new BufferPoolException("Current resource manager is already registered");

        resourceManagers.put(id, manager);

        PageSize pageSize = manager.getPageSize();


        if (!caches.containsKey(pageSize)) {
            PageCache cache =  AbstractExtensionFactory.getExtensionFactory().createPageCache(pageSize, this.config.getCacheSize(pageSize));
            caches.put(pageSize, cache);
            buffers.put(pageSize, new Vector<byte[]>());

            //init all buffers to buffer pool
            for (int i = 0; i < numberIOBuffers; i++) {
                buffers.get(pageSize).add(new byte[pageSize.getNumberOfBytes()]);
            }
        }


    }


    // ------------------------------------------------------------------------
    //                       Page level operations
    // ------------------------------------------------------------------------

    /**
     * Transparently fetches the page defined by the page number for the given resource.
     * If the cache contains the page, it will be fetched from there, otherwise it will
     * be loaded from secondary storage. The page is pinned in order to prevent eviction from
     * the cache.
     * <p/>
     * The binary page will be wrapped by a page object depending on the resource type.
     * If the resource type is for example a table, then the returned object is a <code>TablePage</code>.
     *
     * @param resourceId The id of the resource.
     * @param pageNumber The page number of the page to fetch.
     * @return The requested page, wrapped by a structure to make it accessible.
     * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException         Thrown, if the page had to be loaded from secondary storage and the loading
     *                             failed due to an I/O problem.
     */
    public CacheableData getPageAndPin(int resourceId, int pageNumber)
            throws BufferPoolException, IOException {


        CacheableData page;

        if (!resourceManagers.containsKey(resourceId) || !isOpened)
            throw new BufferPoolException();


        //get manager for this resource
        ResourceManager currentManager = resourceManagers.get(resourceId);
        //get cache for this size
        PageCache currentCache = caches.get(currentManager.getPageSize());
        //get buffers for this size
        PageSize currentPageSize = currentManager.getPageSize();


        //try to read from cache
        synchronized (currentCache) {
            page = currentCache.getPageAndPin(resourceId, pageNumber);
        }


        if (page == null)
            page = getPageFromDisk(resourceId, currentPageSize, pageNumber, currentManager);


        return page;

    }

    /**
     * Unpins a given page and in addition fetches another page from the same resource. This method works exactly
     * like the method {@link de.tuberlin.dima.minidb.io.BufferPoolManager#getPageAndPin(int, int)}, only that it
     * unpins a page before beginning the request for the new page.
     * <p/>
     * Similar to the behavior of {@link de.tuberlin.dima.minidb.io.BufferPoolManager#unpinPage(int, int)}, no exception
     * should be thrown, if the page to be unpinned was already unpinned or no longer in the cache.
     * <p/>
     * This method should perform better than isolated calls to unpin a page and getting a new one.
     * It should, for example, only acquire the lock/monitor on the cache once, instead of twice.
     *
     * @param resourceId      The id of the resource.
     * @param unpinPageNumber The page number of the page to be unpinned.
     * @param getPageNumber   The page number of the page to get and pin.
     * @return The requested page, wrapped by a structure to make it accessible.
     * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException         Thrown, if the page had to be loaded from secondary storage and the loading
     *                             failed due to an I/O problem.
     */
    public CacheableData unpinAndGetPageAndPin(int resourceId, int unpinPageNumber, int getPageNumber)
            throws BufferPoolException, IOException {

        CacheableData page;

        //get
        ResourceManager currentManager = resourceManagers.get(resourceId);
        //get cache for this size
        PageCache currentCache = caches.get(currentManager.getPageSize());

        PageSize currentPageSize = currentManager.getPageSize();

        synchronized (currentCache) {
            currentCache.unpinPage(resourceId, unpinPageNumber);
            page = currentCache.getPageAndPin(resourceId, getPageNumber);
        }


        if (page == null)
            page = writeThread.getRequest(resourceId, getPageNumber);

        if (page == null)
            page = readThread.getRequest(resourceId, getPageNumber);


        if (page == null)
            page = getPageFromDisk(resourceId, currentPageSize, getPageNumber, currentManager);


        return page;


    }

    /**
     * function which retrieves page from the disk and adds to cache
     *
     * @param resourceId
     * @param currentPageSize
     * @param pageNumber
     * @param currentManager
     * @return
     * @throws IOException
     */

    private CacheableData getPageFromDisk(int resourceId, PageSize currentPageSize, int pageNumber, ResourceManager currentManager)
            throws IOException {

        CacheableData page = null;
        byte[] buffer;


        try {

            buffer = getBuffer(currentPageSize);
            Request readRequest = new ReadRequest(resourceId, pageNumber, buffer, currentManager);

            readThread.addRequest(readRequest);
            synchronized (readRequest) {
                while (!readRequest.isCompleted()) {
                    readRequest.wait();
                }
            }

            page = readRequest.getResult();
            addPageToCache(resourceId, page, true);
            putBuffer(currentPageSize);


        } catch (Exception ex) {
            throw new IOException("request interrupted");
        }

        return page;


    }

    /**
     * Unpins a page so that it can again be evicted from the cache. This method works after the principle of
     * <i>best effort</i>: It will try to unpin the page, but will not throw any exception if the page is not
     * contained in the cache or if it is not pinned.
     *
     * @param resourceId The id of the resource.
     * @param pageNumber The page number of the page to unpin.
     */
    public void unpinPage(int resourceId, int pageNumber) {

        //get
        ResourceManager currentManager = resourceManagers.get(resourceId);
        //get cache for this size
        PageCache currentCache = caches.get(currentManager.getPageSize());

        synchronized (currentCache) {
            currentCache.unpinPage(resourceId, pageNumber);
        }

    }

    /**
     * Prefetches a page. If the page is currently in the cache, this method causes a hit on the page.
     * If not, an asynchronous request to load the page is issued. When the asynchronous request has loaded the
     * page, it adds it to the cache without causing it to be considered hit.
     * <p/>
     * The rational behind that behavior is the following: Pages that are already in the cache are hit again and
     * become frequent, which is desirable, since it is in fact accessed multiple times. (A prefetch is typically
     * followed by a getAndPin request short after). Any not yet contained page is added to the cache and not hit,
     * so that the later getAndPin request is in fact the first request and keeps the page among the recent items
     * rather than among the frequent.
     * <p/>
     * This function returns immediately and does not wait for any I/O operation to complete.
     *
     * @param resourceId The id of the resource.
     * @param pageNumber The page number of the page to fetch.
     * @throws BufferPoolException If the buffer pool is closed, or the resource is not registered.
     */
    public void prefetchPage(int resourceId, int pageNumber)
            throws BufferPoolException {

        CacheableData page;
        byte[] buffer;

        if (!resourceManagers.containsKey(resourceId) || !isOpened)
            throw new BufferPoolException();


        //get manager for this resource
        ResourceManager currentManager = resourceManagers.get(resourceId);
        //get cache for this size
        PageCache currentCache = caches.get(currentManager.getPageSize());
        //get buffers for this size
        PageSize currentPageSize = currentManager.getPageSize();


        synchronized (currentCache) {
            page = currentCache.getPage(resourceId, pageNumber); // hit included
        }

        if (page == null) {

            buffer = getBuffer(currentPageSize);
            ReadRequest request = new ReadRequest(resourceId, pageNumber, buffer, currentManager);
            Runnable readOnce = new ReadOnce(request, currentCache);
            new Thread(readOnce).start();
            putBuffer(currentPageSize);
        }


    }

    /**
     * Prefetches a sequence of pages. Behaves exactly like
     * {@link de.tuberlin.dima.minidb.io.BufferPoolManager#prefetchPage(int, int)}, only that it prefetches
     * multiple pages.
     *
     * @param resourceId      The id of the resource.
     * @param startPageNumber The page number of the first page to prefetch.
     * @param endPageNumber   The page number of the last page to prefetch.
     * @throws BufferPoolException If the buffer pool is closed, or the resource is not registered.
     */
    public void prefetchPages(int resourceId, int startPageNumber, int endPageNumber)
            throws BufferPoolException {


        if (!resourceManagers.containsKey(resourceId) || !isOpened)
            throw new BufferPoolException();

        for (int i = startPageNumber; i <= endPageNumber; i++) {
            prefetchPage(resourceId, i);
        }


    }


    /**
     * Creates a new empty page for the resource described by the id and pins the new page. This method is called
     * whenever during the insertion into a table a new page is needed, or index insertion requires an additional
     * page.
     * <p/>
     * The method must take a buffer (from the I/O buffers) and call the ResourceManager
     * {@link de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[])} to create a new page inside
     * that buffer. The resource manager will also reserve and assign a new page number for that page and
     * create the wrapping page (e.g. TablePage or IndexPage).
     *
     * @param type       The resource type, e.g. table or index.
     * @param resourceId The id of the resource for which the page is to be created.
     * @param parameters Parameters for the page creation. Not all resource types will interpret those.
     * @return A new empty page that is part of the given resource.
     * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException         Thrown, if the page had to be loaded from secondary storage and the loading
     *                             failed due to an I/O problem.
     */
    public CacheableData createNewPageAndPin(int resourceId)
            throws BufferPoolException, IOException {


        if (!resourceManagers.containsKey(resourceId) || !isOpened)
            throw new BufferPoolException();

        CacheableData page;
        byte[] buffer;

        ResourceManager currentManager = resourceManagers.get(resourceId);
        PageSize currentPageSize = currentManager.getPageSize();


        buffer = getBuffer(currentPageSize);

        try {
            synchronized (currentManager) {
                page = currentManager.reserveNewPage(buffer);
            }

            addPageToCache(resourceId, page, true);
            putBuffer(currentPageSize);


        } catch (Exception ex) {
            throw new IOException();

        }

        return page;


    }




    /**
     * Creates a new empty page for the resource described by the id and pins the new page. This method is called
     * whenever during the insertion into a table a new page is needed, or index insertion requires an additional
     * page. See {@link de.tuberlin.dima.minidb.io.BufferPoolManager#createNewPageAndPin(int)} for details.
     * <p/>
     * This method takes a parameter that lets the caller specify the type of page to be created. That is important
     * for example for indexes, which have multiple types of pages (leaf pages, inner node pages). The parameter need
     * not be interpreted by the buffer pool manager, but it may rather be passed to the <tt>ResourceManager</tt> of the
     * given resource, who will interpret it and make sure that the correct page type is instantiated.
     *
     * @param type       The resource type, e.g. table or index.
     * @param resourceId The id of the resource for which the page is to be created.
     * @param parameters Parameters for the page creation. Not all resource types will interpret those.
     * @return A new empty page that is part of the given resource.
     * @throws BufferPoolException Thrown, if the given resource it not registered at the buffer pool,
     *                             the buffer pool is closed, or an internal problem occurred.
     * @throws IOException         Thrown, if the page had to be loaded from secondary storage and the loading
     *                             failed due to an I/O problem.
     */
    public CacheableData createNewPageAndPin(int resourceId, Enum<?> type)
            throws BufferPoolException, IOException {

        if (!resourceManagers.containsKey(resourceId) || !isOpened)
            throw new BufferPoolException();

        CacheableData page;
        byte[] buffer;

        ResourceManager currentManager = resourceManagers.get(resourceId);
        PageSize currentPageSize = currentManager.getPageSize();


        buffer = getBuffer(currentPageSize);

        try {
            synchronized (currentManager) {
                page = currentManager.reserveNewPage(buffer, type);
            }

            addPageToCache(resourceId, page, true);
            putBuffer(currentPageSize);

        } catch (Exception ex) {
            throw new IOException();
        }

        return page;

    }


    /**
     * Returns a buffer (byte array) of size <code>BUFFER_SIZE from the
     * pool. When the buffer is no longer needed, it should be put back in the
     * pool for future use by calling <code>putBuffer.
     *
     * @return a buffer
     * @see #putBuffer(byte[])
     */

    private byte[] getBuffer(PageSize size) throws BufferPoolException {

        Vector<byte[]> availableBuffers = buffers.get(size);

        byte[] buffer;

        try {
            synchronized (availableBuffers) {
                while (availableBuffers.isEmpty()) {
                    availableBuffers.wait();
                }
                buffer = availableBuffers.lastElement();
                availableBuffers.remove(buffer);
                availableBuffers.notifyAll();
            }

        } catch (InterruptedException ie) {
            throw new BufferPoolException();
        }

        return buffer;

    }


    /**
     * Puts the given buffer into the pool for future use.
     *
     * @param buffer the buffer to be put back into the buffer pool
     * @see #getBuffer()
     */
    public void putBuffer(PageSize size) {

        Vector<byte[]> bufferPool = buffers.get(size);

        if (bufferPool.size() < config.getNumIOBuffers()) {
            synchronized (bufferPool) {

                bufferPool.add(new byte[size.getNumberOfBytes()]);
                bufferPool.notifyAll();
            }

        }
    }

    /**
     * Adding page to cache
     *
     * @param resourceId
     * @param page
     * @param pin
     */

    private void addPageToCache(int resourceId, CacheableData page, boolean isPinned) {

        ResourceManager currentManager = resourceManagers.get(resourceId);
        PageSize currentPageSize = currentManager.getPageSize();
        PageCache currentCache = caches.get(currentPageSize);
        byte[] buffer;

        EvictedCacheEntry writePage;

        try {
            synchronized (currentCache) {
                if (isPinned)
                    writePage = currentCache.addPageAndPin(page, resourceId);
                else
                    writePage = currentCache.addPage(page, resourceId);
            }

            CacheableData writeData = writePage.getWrappingPage();

            int evictedResourceId = writePage.getResourceID();
            ResourceManager evictedManager = resourceManagers.get(evictedResourceId);

            if (writeData != null && writeData.hasBeenModified()) {
                buffer = getBuffer(currentPageSize);
                buffer = writeData.getBuffer();
                WriteRequest writeRequest = new WriteRequest(evictedResourceId, writeData.getPageNumber(), buffer, evictedManager, writeData);
                writeThread.addRequest(writeRequest);
                putBuffer(currentPageSize);
            }

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }


}
