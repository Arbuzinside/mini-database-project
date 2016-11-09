package de.tuberlin.dima.minidb.io.index;

import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DuplicateException;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Created by arbuzinside on 18.11.2015.
 */
public class MyBTreeIndex implements BTreeIndex {


    private final int leafSize = 5;
    private final int nodeSize = 3;
    private IndexSchema schema;
    private BufferPoolManager poolManager;
    private int resourceId;
    private LinkedList<BTreeLeafPage> leafs;
    private LinkedList<BTreeInnerNodePage> nodes;


    public MyBTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {

        this.schema = schema;
        this.resourceId = resourceId;
        this.poolManager = bufferPool;
        leafs = new LinkedList<>();
        nodes = new LinkedList<>();

    }


    /**
     * Gets the schema of the index represented by this instance.
     *
     * @return The schema of the index.
     */
    public IndexSchema getIndexSchema() {

        return schema;

    }

    /**
     * Gets all RIDs for the given key. If the key is not found, then the returned iterator will
     * not return any element (the first call to hasNext() is false).
     * <p/>
     * This method should in general not get all RIDs for that key from the index at once, but only
     * some. If the sequence of RIDs for that key spans multiple pages, these pages should be loaded gradually
     * and the RIDs should be extracted when needed. It makes sense to always extract all RIDs from one page
     * in one step, in order to be able to unpin that page again.
     * <p/>
     * Consider an example, where the key has many RIDs, spanning three pages. The iterator should load those
     * from the first page first. When they are all returned, it loads the next page, extracting all RIDs there,
     * returning them one after the other, and so on. It makes sense to issue prefetch requests for the next leaf
     * pages ahead.
     *
     * @param key The key to get the RIDs for.
     * @return An Iterator over of all RIDs for key.
     * @throws PageFormatException         Thrown if during processing a page's layout was found to be
     *                                     found to be corrupted.
     * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
     *                                     of the BTree were found to be invalid.
     * @throws IOException                 Thrown, if a page could not be loaded.
     */
    public IndexResultIterator<RID> lookupRids(DataField key)
            throws PageFormatException, IndexFormatCorruptException, IOException {

        BTreeLeafPage currentLeaf, newLeaf;
        BTreeInnerNodePage currentNode, newNode;
        CacheableData page;
        int nextPageNumber, currentPageNumber;
        ArrayList<RID> ridList = new ArrayList<>();
        nodes = new LinkedList<>();
        leafs = new LinkedList<>();
        //getting page to start
        currentPageNumber = schema.getFirstLeafNumber();
        //always make the first iteration
        nextPageNumber = 0;
        ArrayList<RID> rids = new ArrayList<>();
        while (nextPageNumber != -1) {

            try {
                //getting the first leaf from schema
                currentLeaf = (BTreeLeafPage) poolManager.getPageAndPin(resourceId, currentPageNumber);
                //getting the rid from the leafs
                currentLeaf.getAllsRIDsForKey(key, ridList);
                //if no rids for the key, go to next page
                if (ridList.isEmpty()) {
                    nextPageNumber = currentLeaf.getNextLeafPageNumber();
                    currentPageNumber = nextPageNumber;
                    poolManager.unpinPage(resourceId, currentPageNumber);
                    continue;
                }
                //sort rids
                Collections.sort(ridList);
                //put pageNumbers for next iteration
                nextPageNumber = currentLeaf.getNextLeafPageNumber();
                currentPageNumber = nextPageNumber;
                //test
                rids.addAll(ridList);
                //
                ridList.clear();

            } catch (BufferPoolException ex) {
                ex.printStackTrace();
            }
        }

        return buildTree(key, rids);
    }


    private MyIndexRIDIterator buildTree(DataField key, ArrayList<RID> rids) {

        boolean exitLeaf = false;
        boolean exitNode = false;
        LinkedList<BTreeLeafPage> treeLeafs = new LinkedList<>();
        LinkedList<BTreeInnerNodePage> innerNodes = new LinkedList<>();

        try {
            for (int i = 0; i < rids.size(); i += leafSize) {

                BTreeLeafPage currentLeaf = (BTreeLeafPage) poolManager.createNewPageAndPin(resourceId, BTreeIndexPageType.LEAF_PAGE);
                for (int j = i; j < i + leafSize; j++) {
                    if (rids.size() > j)
                        currentLeaf.insertKeyRIDPair(key, rids.get(j));
                    else {
                        exitLeaf = true;
                        break;
                    }
                }
                treeLeafs.add(currentLeaf);
                poolManager.unpinPage(resourceId, currentLeaf.getPageNumber());
                if (exitLeaf) break;
            }
        } catch (Exception ex) {
            //bla
        }


        if (treeLeafs.size() == 1)
            return new MyIndexRIDIterator(treeLeafs.get(0), key);
        try {
            for (int i = 0; i < treeLeafs.size(); i += nodeSize) {
                BTreeInnerNodePage currentNode = (BTreeInnerNodePage) poolManager.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
                for (int j = i; j < nodeSize + i; j++) {
                    if (treeLeafs.size() > j)
                        currentNode.insertKeyPageNumberPairAtPosition(key, treeLeafs.get(j).getPageNumber(), j);
                        //(key, treeLeafs.get(j).getPageNumber());
                    else {
                        exitNode = true;
                        break;
                    }
                    innerNodes.add(currentNode);
                    poolManager.unpinPage(resourceId, currentNode.getPageNumber());
                    if (exitNode) break;
                }
            }

            //if the root level
            int isRoot = (innerNodes.size() >= 2) ? 0 : 1;

            switch (isRoot) {
                case (1):
                    BTreeInnerNodePage rootPage = (BTreeInnerNodePage) poolManager.createNewPageAndPin(resourceId, BTreeIndexPageType.INNER_NODE_PAGE);
                    rootPage.initRootState(key, innerNodes.getFirst().getPointer(0), innerNodes.getLast().getPointer(1));
                    break;
                case (0):
                    break;
            }


        } catch (Exception ex) {
            //
        }

        return new MyIndexRIDIterator(treeLeafs, innerNodes, key);
    }


    /**
     * Gets all RIDs in a given key-range. The rage is defined by the start key <code>l</code> (lower bound)
     * and the stop key <code>u</code> (upper bound), where both <code>l</code> and <code>u</code> can be
     * optionally included or excluded from the interval, e.g. [l, u) or [l, u].
     * <p/>
     * This method should obey the same on-demand-loading semantics as the {@link #lookupRids(DataField)} method. I.e. it
     * should NOT first retrieve all RIDs and then return an iterator over an internally kept list.
     *
     * @param startKey         The lower boundary of the requested interval.
     * @param stopKey          The upper boundary of the requested interval.
     * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary.
     * @param stopKeyIncluded  A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
     * @return An Iterator over of all RIDs for the given key range.
     * @throws PageFormatException         Thrown if during processing a page's layout was found to be
     *                                     found to be corrupted.
     * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
     *                                     of the BTree were found to be invalid.
     * @throws IOException                 Thrown, if a page could not be loaded.
     */
    public IndexResultIterator<RID> lookupRids(DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
            throws PageFormatException, IndexFormatCorruptException, IOException {


        BTreeLeafPage currentLeaf, newLeaf;
        BTreeInnerNodePage currentNode, newNode;
        CacheableData page;
        int nextPageNumber, currentPageNumber;
        ArrayList<RID> ridList = new ArrayList<>();
        nodes = new LinkedList<>();
        leafs = new LinkedList<>();
        //getting page to start
        currentPageNumber = schema.getFirstLeafNumber();
        //always make the first iteration
        nextPageNumber = 0;
        ArrayList<RID> rids = new ArrayList<>();
        while (nextPageNumber != -1) {

            try {
                //getting the first leaf from schema
                currentLeaf = (BTreeLeafPage) poolManager.getPageAndPin(resourceId, currentPageNumber);
                //getting the rid from the leafs
                for (int i = 0; i < currentLeaf.getNumberOfEntries(); i++) {
                    if ((currentLeaf.getKey(i).compareTo(startKey) == 0 && startKeyIncluded || currentLeaf.getKey(i).compareTo(startKey) > 0)
                            && (currentLeaf.getKey(i).compareTo(stopKey) < 0 || (currentLeaf.getKey(i).compareTo(stopKey) == 0 && stopKeyIncluded)))
                        rids.add(currentLeaf.getRidAtPosition(i));
                }

                // currentLeaf.getAllsRIDsForKey(startKey, ridList);
                //if no rids for the key, go to next page
                if (ridList.isEmpty()) {
                    nextPageNumber = currentLeaf.getNextLeafPageNumber();
                    currentPageNumber = nextPageNumber;
                    poolManager.unpinPage(resourceId, currentPageNumber);
                    continue;
                }
                //sort rids
                Collections.sort(ridList);
                //put pageNumbers for next iteration
                nextPageNumber = currentLeaf.getNextLeafPageNumber();
                currentPageNumber = nextPageNumber;
                //test
                rids.addAll(ridList);
                //
                ridList.clear();

            } catch (BufferPoolException ex) {
                ex.printStackTrace();
            }
        }

        return new MyIndexRIDIterator(rids);
        //return buildTree(startKey, rids);
    }


    /**
     * Gets all Keys that are contained in the given key-range. The rage is defined by the start key <code>l</code> (lower bound)
     * and the stop key <code>u</code> (upper bound), where both <code>l</code> and <code>u</code> can be
     * optionally included or excluded from the interval, e.g. [l, u) or [l, u].
     * <p/>
     * This method should obey the same on-demand-loading semantics as the {@link #lookupRids(DataField)} method. I.e. it
     * should NOT first retrieve all RIDs and then return an iterator over an internally kept list.
     *
     * @param startKey         The lower boundary of the requested interval.
     * @param stopKey          The upper boundary of the requested interval.
     * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary.
     * @param stopKeyIncluded  A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
     * @return An Iterator over of all RIDs for the given key range.
     * @throws PageFormatException         Thrown if during processing a page's layout was found to be
     *                                     found to be corrupted.
     * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
     *                                     of the BTree were found to be invalid.
     * @throws IOException                 Thrown, if a page could not be loaded.
     */
    public IndexResultIterator<DataField> lookupKeys(DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded)
            throws PageFormatException, IndexFormatCorruptException, IOException {

        BTreeLeafPage currentLeaf, newLeaf;
        BTreeInnerNodePage targetNode, newNode;
        CacheableData page;
        int nextPageNumber, currentPageNumber;
        ArrayList<DataField> ridList = new ArrayList<>();
        nodes = new LinkedList<>();
        leafs = new LinkedList<>();
        //getting page to start
        currentPageNumber = schema.getFirstLeafNumber();
        //always make the first iteration
        nextPageNumber = 0;
        ArrayList<DataField> rids = new ArrayList<>();
        while (nextPageNumber != -1) {

            try {
                //getting the first leaf from schema
                currentLeaf = (BTreeLeafPage) poolManager.getPageAndPin(resourceId, currentPageNumber);
                //getting the rid from the leafs
                for (int i = 0; i < currentLeaf.getNumberOfEntries(); i++) {
                    if ((currentLeaf.getKey(i).compareTo(startKey) == 0 && startKeyIncluded || currentLeaf.getKey(i).compareTo(startKey) > 0)
                            && (currentLeaf.getKey(i).compareTo(stopKey) < 0 || (currentLeaf.getKey(i).compareTo(stopKey) == 0 && stopKeyIncluded)))
                        rids.add(currentLeaf.getKey(i));
                }

                // currentLeaf.getAllsRIDsForKey(startKey, ridList);
                //if no rids for the key, go to next page
                if (ridList.isEmpty()) {
                    nextPageNumber = currentLeaf.getNextLeafPageNumber();
                    currentPageNumber = nextPageNumber;
                    poolManager.unpinPage(resourceId, currentPageNumber);
                    continue;
                }
                //sort rids
                Collections.sort(ridList);
                //put pageNumbers for next iteration
                nextPageNumber = currentLeaf.getNextLeafPageNumber();
                currentPageNumber = nextPageNumber;
                //test
                rids.addAll(ridList);
                //
                ridList.clear();

            } catch (BufferPoolException ex) {
                ex.printStackTrace();
            }
        }

        return new MyIndexDataFieldIterator(rids);
        //return buildTree(startKey, rids);
    }


    /**
     * Inserts a pair of (key/RID) into the index. For unique indexes, this method must throw
     * a DuplicateException, if the key is already contained.
     * <p/>
     * If the page number of the root node or the first leaf node changes during the operation,
     * then this method must notify the schema to reflect this change.
     *
     * @param key The key of the pair to be inserted.
     * @param rid The RID of the pair to be inserted.
     * @throws PageFormatException         Thrown if during processing a page's layout was found to be
     *                                     found to be corrupted.
     * @throws IndexFormatCorruptException Throws, if the evaluation failed because condition
     *                                     of the BTree were found to be invalid.
     * @throws DuplicateException          Thrown, if the key is already contained and the index is defined to be unique.
     * @throws IOException                 Thrown, if a page could not be read or written.
     */
    public void insertEntry(DataField key, RID rid)
            throws PageFormatException, IndexFormatCorruptException, DuplicateException, IOException {
    }

}






