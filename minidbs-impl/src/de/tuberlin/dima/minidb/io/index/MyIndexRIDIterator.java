package de.tuberlin.dima.minidb.io.index;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by arbuzinside on 26.11.2015.
 */
public class MyIndexRIDIterator implements IndexResultIterator<RID>{
    private LinkedList<BTreeInnerNodePage> nodes;
    private LinkedList<BTreeLeafPage> leafs;

    public ArrayList<RID> rids;
    private BTreeLeafPage currentLeaf;
    private int pointer;

    private BufferPoolManager poolManager;

    public MyIndexRIDIterator(){

    }


    public MyIndexRIDIterator(LinkedList<BTreeLeafPage> leafs, LinkedList<BTreeInnerNodePage> nodes, DataField key){
        this.leafs = leafs;
        this.nodes = nodes;
        this.pointer = 0;
        this.currentLeaf = leafs.getFirst();
        rids = new ArrayList<>();
        try {
            for (BTreeLeafPage leaf : leafs)
                leaf.getAllsRIDsForKey(key, rids);
        }catch(PageFormatException ex){
            //bla bla
        }
    }

    public MyIndexRIDIterator(BTreeLeafPage leaf, DataField key){
       this.currentLeaf = leaf;
        this.rids = new ArrayList<>();


        try {
                leaf.getAllsRIDsForKey(key, rids);
        }catch(PageFormatException ex){
            //bla bla
        }

    }

    public void insertAtPosition(RID rid, int position){

    }

    public MyIndexRIDIterator(ArrayList<RID> rids){

        this.rids = rids;
    }

    /**
     * This method checks, if further elements are available from this iterator.
     *
     * @return true, if there are more elements available, false if not.
     * @throws IOException Thrown, if the method fails due to an I/O problem.
     * @throws IndexFormatCorruptException Thrown, if the method fails because the index is in an inconsistent state.
     * @throws PageFormatException Thrown, if a corrupt page prevents execution of this method.
     */
    public boolean hasNext() throws IOException, IndexFormatCorruptException, PageFormatException{

        return pointer < rids.size() && rids.get(pointer) != null;




    }

    public int getPointer(){
        return pointer;
    }


    /**
     * This gets the next element from the iterator, moving the iterator forward.
     * This method should succeed, if a prior call to hasNext() returned true.
     *
     * @return The next element in the sequence.
     * @throws IOException Thrown, if the method fails due to an I/O problem.
     * @throws IndexFormatCorruptException Thrown, if the method fails because the index is in an inconsistent state.
     * @throws PageFormatException Thrown, if a corrupt page prevents execution of this method.
     */
    @Override
    public RID next() throws IOException, IndexFormatCorruptException, PageFormatException{

        return rids.get(pointer++);
    }




}
