package de.tuberlin.dima.minidb.io.cache;



import java.util.*;

/**
 * Created by arbuzinside on 3.11.2015.
 */
public class MyPageCache implements PageCache {


    private int pageSize;
    private int bufferSize;

    private LinkedHashMap<CacheId, MyCacheableData> LR;
    private LinkedHashMap<CacheId, MyCacheableData> LF;



    private Set<CacheId> deletedLR;
    private Set<CacheId> deletedLF;


    private boolean hasDeleted;




    public MyPageCache(PageSize page, int pages){

        this.pageSize = page.getNumberOfBytes();
        this.bufferSize = pages;



        LR = new LinkedHashMap<>();
        LF = new LinkedHashMap<>();


        hasDeleted = false;

        deletedLR = new HashSet<>();
        deletedLF = new HashSet<>();


    }

    /**
     * Checks, if a page is in the cache and returns the cache entry for it. The
     * page is identified by its resource id and the page number. If the
     * requested page is not in the cache, this method returns null.
     * The page experiences a cache hit through the request.
     *
     * @param resourceId The id of the resource for which we seek to get a page.
     * @param pageNumber The physical page number of the page we seek to retrieve.
     * @return The cache entry containing the page data, or null, if the page is not
     *         contained in the cache.
     */
    public CacheableData getPage(int resourceId, int pageNumber){


        CacheId currentKey = new CacheId(pageNumber, resourceId);

        MyCacheableData page;

        if (LR.containsKey(currentKey) && !LR.get(currentKey).isDeleted()){

            page = LR.remove(currentKey);

            page.hitPage();

            if ( page.getHits() == 2)
                LF.put(currentKey, page);
            else
                LR.put(currentKey, page);
            return page.getPage();
        } else if (LF.containsKey(currentKey) && !LF.get(currentKey).isDeleted()){


            page = LF.remove(currentKey);

            page.hitPage();
            LF.put(currentKey, page);
            return page.getPage();
        } else
            return null;
    }

    /**
     * Checks, if a page is in the cache and returns the cache entry for it. Upon success,
     * it also increases the pinning counter of the entry for the page such that the entry
     * cannot be evicted from the cache until the pinning counter reaches zero.
     * <p>
     * The page is identified by its resource id and the page number. If the
     * requested page is not in the cache, this method returns null.
     * The page experiences a cache hit through the request.
     *
     * @param resourceId The id of the resource for which we seek to get a page.
     * @param pageNumber The physical page number of the page we seek to retrieve.
     * @return The cache entry containing the page data, or null, if the page is not
     *         contained in the cache.
     */
    public CacheableData getPageAndPin(int resourceId, int pageNumber){





        CacheId currentKey = new CacheId(pageNumber, resourceId);

        MyCacheableData page;

        if (LR.containsKey(currentKey) && !LR.get(currentKey).isDeleted()){

            page = LR.remove(currentKey);

            page.hitPage();
            page.pinPage();

            if ( page.getHits() == 2)
                LF.put(currentKey, page);
            else
                LR.put(currentKey, page);
            return page.getPage();
        } else if (LF.containsKey(currentKey) && !LF.get(currentKey).isDeleted()){


            page = LF.remove(currentKey);
            page.hitPage();
            page.pinPage();
            LF.put(currentKey, page);
            return page.getPage();
        } else
            return null;


    }

    /**
     * This method adds a page to the cache by adding a cache entry for it. The entry must not be
     * already contained in the cache. In order to add the new entry, one entry will always be
     * evicted to keep the number of contained entries constant.
     * <p>
     * If the cache is still pretty cold (new) that the evicted entry may be an
     * entry containing no resource data. In any way does the evicted entry contain a binary page
     * buffer.
     * <p>
     * For ARC, if the page was not in any of the bottom lists (B lists), the page enters the MRU end of the
     * T1 list. The newly added page is not considered as hit yet. Therefore the first getPage() request to that
     * page produces the first hit and does not move the page out of the T1 list. This functionality is important
     * to allow pre-fetched pages to be added to the cache without causing the first actual "GET" request to
     * mark them as frequent.
     * If the page was contained in any of the Bottom lists before, it will directly enter the T2 list (frequent
     * items). Since it is directly considered frequent, it is considered to be hit.
     *
     * @param newPage The new page to be put into the cache.
     * @param resourceId The id of the resource the page belongs to.
     *
     * @return The entry for the page that needed to be evicted.
     * @throws CachePinnedException Thrown, if no page could be evicted, because all pages
     *                              are pinned.
     * @throws DuplicateCacheEntryException Thrown, if an entry for that page is already contained.
     *                                      The entry is considered to be already contained, if an
     *                                      entry with the same resource-type, resource-id and page
     *                                      number is in the cache. The contents of the binary page
     *                                      buffer does not need to match for this exception to be
     *                                      thrown.
     */
   public EvictedCacheEntry addPage(CacheableData newPage, int resourceId)
            throws CachePinnedException, DuplicateCacheEntryException{



       CacheId currentKey = new CacheId(newPage.getPageNumber(), resourceId);
       MyCacheableData currentPage = new MyCacheableData(newPage, resourceId);
       EvictedCacheEntry deleted;


       if (LR.containsKey(currentKey) || LF.containsKey(currentKey))
           throw new DuplicateCacheEntryException(resourceId, newPage.getPageNumber());


       //check if the page is in deleted lists
       if (deletedLR.remove(currentKey) || deletedLF.remove(currentKey)){

            currentPage.hitPage();

           if (LR.size() + LF.size() >= bufferSize && LR.size() < LF.size()) {
               deleted = removeElement(LF);
               LF.put(currentKey,currentPage);
               return deleted;
           } else if (LR.size() + LF.size() >= bufferSize && LR.size() >= LF.size()){
               deleted = removeElement(LR);
               LF.put(currentKey,currentPage);
               return deleted;
           }
       }






        // checks if the page is in LR list
       if(LR.get(currentKey) != null){

           LR.get(currentKey).hitPage();

           //hits >= 2
           if (LR.get(currentKey).getHits() == 2){
               LR.remove(currentKey);
               LF.put(currentKey, currentPage);

               return new EvictedCacheEntry(new byte[pageSize]);

               //hits == 2 and LR is full
           } else if(LR.size() + LF.size() >= bufferSize && LR.size() >= LF.size()){

               deleted = removeElement(LR);
               LR.put(currentKey,currentPage);
               return deleted;

               //hits == 2 and we have space
           } else if(LR.size() < bufferSize - LF.size()){


               LR.put(currentKey, currentPage);
               deleted = new EvictedCacheEntry(new byte[pageSize]);

               return deleted;
           }
        // check if the page is in LF
       } else if (LF.get(currentKey) != null){
             LF.get(currentKey).hitPage();

           //if the buffer is full
           if (LF.size() == bufferSize - LR.size()){

               deleted = removeElement(LF);
               LR.put(currentKey,currentPage);
               return deleted;
            //if we have space
           } else {

               LR.put(currentKey, currentPage);
               deleted = new EvictedCacheEntry(new byte[pageSize]);

               return deleted;
           }

       } else if (LR.size() + LF.size() >= bufferSize && LR.size() < LF.size()) {
           deleted = removeElement(LF);
           LR.put(currentKey,currentPage);
           return deleted;
       } else if (LR.size() + LF.size() >= bufferSize && LR.size() >= LF.size()){
           deleted = removeElement(LR);
           LR.put(currentKey,currentPage);
           return deleted;
       }

       LR.put(currentKey,currentPage);
       return new EvictedCacheEntry(new byte[pageSize]);

    }


    //delete the last element from the buffer side

    private EvictedCacheEntry removeElement(LinkedHashMap<CacheId, MyCacheableData> side) throws CachePinnedException {





            if (side == LR)
                return removefromLR();
             else if (side == LF)
                return removefromLF();
             else
                return new EvictedCacheEntry(new byte[pageSize]);

    }


    private EvictedCacheEntry removefromLR() throws CachePinnedException {


        EvictedCacheEntry deleted;

        Map.Entry<CacheId, MyCacheableData> entry;
        Iterator it;



        if (hasDeleted) {
            it = LR.entrySet().iterator();

            while (it.hasNext()) {
                entry = (Map.Entry<CacheId, MyCacheableData>) it.next();

                if (entry.getValue().isDeleted()) {
                    CacheableData page = entry.getValue().getPage();
                    it.remove();
                    return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
                }
            }
        }



        it = LR.entrySet().iterator();
        while (it.hasNext()) {

            entry = (Map.Entry<CacheId, MyCacheableData>) it.next();

            if (entry.getValue().isPinned() == 0) {


                deleted = new EvictedCacheEntry(entry.getValue().getPage().getBuffer(), entry.getValue().getPage(), entry.getKey().getResourceId());
                LR.remove(entry.getKey());


                if (deletedLR.size() > LF.size()){
                    deletedLR.remove(0);
                    deletedLR.add(entry.getKey());
                }
                else {
                    deletedLR.add(entry.getKey());
                }
                return deleted;

            }
        }

            it = LF.entrySet().iterator();
            while (it.hasNext()) {

                entry = (Map.Entry<CacheId, MyCacheableData>) it.next();

                if (entry.getValue().isPinned() == 0) {
                    deleted = new EvictedCacheEntry(entry.getValue().getPage().getBuffer(), entry.getValue().getPage(), entry.getKey().getResourceId());
                    LF.remove(entry.getKey());

                    if (deletedLF.size() > 2*bufferSize - LF.size()){
                        deletedLF.remove(0);
                        deletedLF.add(entry.getKey());
                    }
                    else {
                        deletedLF.add(entry.getKey());
                    }
                    return deleted;

                }
            }

       throw new CachePinnedException();

    }

    private EvictedCacheEntry removefromLF() throws CachePinnedException{


        EvictedCacheEntry deleted;

        Map.Entry<CacheId, MyCacheableData> entry;
        Iterator it;


        if (hasDeleted) {
            it = LF.entrySet().iterator();

            while (it.hasNext()) {
                entry = (Map.Entry<CacheId, MyCacheableData>) it.next();

                if (entry.getValue().isDeleted()) {
                    CacheableData page = entry.getValue().getPage();
                    it.remove();
                    return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
                }
            }
        }





        it = LF.entrySet().iterator();
        while (it.hasNext()) {

            entry = (Map.Entry<CacheId, MyCacheableData>) it.next();

            if (entry.getValue().isPinned() == 0) {
                deleted = new EvictedCacheEntry(entry.getValue().getPage().getBuffer(), entry.getValue().getPage(), entry.getKey().getResourceId());
                LF.remove(entry.getKey());

                if (deletedLF.size() > 2*bufferSize - LF.size()){
                    deletedLF.remove(0);
                    deletedLF.add(entry.getKey());
                }
                else {
                    deletedLF.add(entry.getKey());
                }

                return deleted;

            }
        }

        it = LR.entrySet().iterator();
        while (it.hasNext()) {

            entry = (Map.Entry<CacheId, MyCacheableData>) it.next();

            if (entry.getValue().isPinned() == 0) {

                deleted = new EvictedCacheEntry(entry.getValue().getPage().getBuffer(), entry.getValue().getPage(), entry.getKey().getResourceId());
                LR.remove(entry.getKey());
                if (deletedLR.size() > LF.size()){
                    deletedLR.remove(0);
                    deletedLR.add(entry.getKey());
                }
                else {
                    deletedLR.add(entry.getKey());
                }

                return deleted;

            }
        }

        throw new CachePinnedException();

    }






    /**
     * This method behaves very similar to the  <code>addPage(CacheEntry)</code> method, with the
     * following distinctions:
     * 1) The page is immediately pinned. (Increase pinning counter, see unpinPage for further information.)
     * 2) The page is immediately considered to be hit, even if it enters the T1 list.
     *
     * @param newPage The new page to be put into the cache.
     * @param resourceId The id of the resource the page belongs to.
     *
     * @return The entry for the page that needed to be evicted.
     * @throws CachePinnedException Thrown, if no page could be evicted, because all pages
     *                              are pinned.
     * @throws DuplicateCacheEntryException Thrown, if an entry for that page is already contained.
     *                                      The entry is considered to be already contained, if an
     *                                      entry with the same resource-type, resource-id and page
     *                                      number is in the cache. The contents of the binary page
     *                                      buffer does not need to match for this exception to be
     *                                      thrown.
     */
   public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId)
            throws CachePinnedException, DuplicateCacheEntryException{



       CacheId currentKey = new CacheId(newPage.getPageNumber(), resourceId);
       MyCacheableData currentPage = new MyCacheableData(newPage, resourceId);
       EvictedCacheEntry deleted;


       if (LR.containsKey(currentKey) || LF.containsKey(currentKey))
           throw new DuplicateCacheEntryException(resourceId, newPage.getPageNumber());




       currentPage.pinPage();
       currentPage.hitPage();




       //check if the page is in deleted lists
       if (deletedLR.remove(currentKey) || deletedLF.remove(currentKey)){

           currentPage.hitPage();

           if (LR.size() + LF.size() >= bufferSize && LR.size() < LF.size()) {
               deleted = removeElement(LF);
               LF.put(currentKey,currentPage);
               return deleted;
           } else if (LR.size() + LF.size() >= bufferSize && LR.size() >= LF.size()){
               deleted = removeElement(LR);
               LF.put(currentKey,currentPage);
               return deleted;
           }
       }






       // checks if the page is in LR list
       if(LR.get(currentKey) != null){

           LR.get(currentKey).hitPage();

           //hits >= 2
           if (LR.get(currentKey).getHits() == 2){
               LR.remove(currentKey);
               LF.put(currentKey, currentPage);

               return new EvictedCacheEntry(new byte[pageSize]);

               //hits == 2 and LR is full
           } else if(LR.size() + LF.size() >= bufferSize && LR.size() >= LF.size()){


               deleted = removeElement(LR);
               LR.put(currentKey,currentPage);

               return deleted;

               //hits == 2 and we have space
           } else if(LR.size() < bufferSize - LF.size()){


               LR.put(currentKey, currentPage);
               deleted = new EvictedCacheEntry(new byte[pageSize]);

               return deleted;
           }
           // check if the page is in LF
       } else if (LF.get(currentKey) != null){
           LF.get(currentKey).hitPage();

           //if the buffer is full
           if (LF.size() == bufferSize - LR.size()){

               deleted = removeElement(LF);
               LR.put(currentKey,currentPage);
               return deleted;
               //if we have space
           } else {

               LR.put(currentKey, currentPage);
               deleted = new EvictedCacheEntry(new byte[pageSize]);

               return deleted;
           }

       } else if (LR.size() + LF.size() >= bufferSize && LR.size() < LF.size()) {
           deleted = removeElement(LF);
           LR.put(currentKey,currentPage);
           return deleted;
       } else if (LR.size() + LF.size() >= bufferSize && LR.size() >= LF.size()){
           deleted = removeElement(LR);
           LR.put(currentKey,currentPage);
           return deleted;
       }

       LR.put(currentKey,currentPage);
       return new EvictedCacheEntry(new byte[pageSize]);



    }

    /**
     * Decreases the pinning counter of the entry for the page described by this resource-id and
     * page number. If there is no entry for this page, this method does nothing. If
     * the entry is not pinned, this method does nothing. If the pinning counter reaches zero, the
     * page can be evicted from the cache.
     *
     * @param resourceId The id of the resource of the page entry to be unpinned.
     * @param pageNumber The physical page number of the page entry to be unpinned.
     */
    @Override
    public void unpinPage(int resourceId, int pageNumber){

        CacheId currentKey = new CacheId(pageNumber, resourceId);

        if(LR.containsKey(currentKey)){

            if(LR.get(currentKey).isPinned() > 0)
                LR.get(currentKey).unpinPage();

        }

        if (LF.containsKey(currentKey)){

            if(LF.get(currentKey).isPinned() > 0)
                LF.get(currentKey).unpinPage();
        }

    }



    /**
     * Gets all pages/entries for the resource specified by this resource-type and
     * resource-id. A call to this method counts as a request for each individual pages
     * entry in the sense of how it affects the replacement strategy.
     *
     * @param resourceId The id of the resource for which we seek the pages.
     * @return An array of all pages for this resource, or an empty array, if no page is
     *         contained for this resource.
     */
    public CacheableData[] getAllPagesForResource(int resourceId){


        ArrayList<CacheableData> data = new ArrayList<>();


        for(Map.Entry<CacheId, MyCacheableData> entry: LR.entrySet()){

            if(entry.getKey().getResourceId() == resourceId){
                data.add(entry.getValue().getPage());
            }

        }

        for(Map.Entry<CacheId, MyCacheableData> entry: LF.entrySet()){

            if(entry.getKey().getResourceId() == resourceId){
                data.add(entry.getValue().getPage());
            }

        }
        CacheableData[] result = new CacheableData[data.size()];
        data.toArray(result);

        return  result;
    }

    /**
     * Removes all entries/pages belonging to a specific resource from the cache. This method
     * does not obey pinning: Entries that are pinned are also expelled. The expelled pages
     * may no longer be found by the <code>getPage()</code> or <code>getPageAndPin</code>.
     * The entries for the expelled pages will be the next to be evicted from the cache.
     * If no pages from the given resource are contained in the cache, this method does
     * nothing.
     *
     * NOTE: This method must not change the size of the cache. It also is not required to
     * physically destroy the entries for the expelled pages - they simply must no longer
     * be retrievable from the cache and be the next to be replaced. The byte arrays
     * behind the expelled pages must be kept in the cache and be returned as evicted
     * entries once further pages are added to the cache.
     *
     * @param type The type of the resource whose pages are to be expelled.
     * @param resourceId The id of the resource whose pages are to be replaced.
     */
    public void expellAllPagesForResource(int resourceId){

        for(Map.Entry<CacheId, MyCacheableData> entry: LR.entrySet()){

            if(entry.getKey().getResourceId() == resourceId){
                entry.getValue().setDeleted(true);
            }

        }
        for(Map.Entry<CacheId, MyCacheableData> entry: LF.entrySet()){

            if(entry.getKey().getResourceId() == resourceId){
                 entry.getValue().setDeleted(true);
            }

        }

        hasDeleted = true;


    }

    /**
     * Gets the capacity of this cache in pages (entries).
     *
     * @return The number of pages (cache entries) that this cache can hold.
     */
    public int getCapacity(){

        return bufferSize;
    }

    /**
     * Unpins all entries, such that they can now be evicted from the cache (pinning counter = 0).
     * This operation has no impact on the position of the entry in the structure that
     * decides which entry to evict next.
     */
    public void unpinAllPages(){

        for(Map.Entry<CacheId, MyCacheableData> entry: LR.entrySet()){

            if(entry.getValue().isPinned() > 0){
                entry.getValue().setPinToZero();
            }

        }
        for(Map.Entry<CacheId, MyCacheableData> entry: LF.entrySet()){

            if(entry.getValue().isPinned() > 0){
                entry.getValue().setPinToZero();
            }

        }

    }


}
