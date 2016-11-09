package de.tuberlin.dima.minidb.optimizer.cost;

import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;


/**
 * Created by arbuzinside on 2.3.2016.
 *
 */


/**
 * A simple interface for computing operator costs. All internal computations
 * depend on four parameters
 *
 *  <li /> readCost - The default time (nanoseconds) that is needed to
 *  transfer a block of the default block size from secondary storage to main
 *  memory.
 *
 *  <li /> writeCost - default time (nanoseconds) that is needed to transfer a
 *  block of the default block size from main memory to secondary storage.
 *
 *  <li /> randomReadOverHead - the overhead for a single block read operation
 *  if the block is not part of a sequence that is read. For magnetic disks,
 *  that would correspond to seek time + rotational latency.
 *
 *  <li /> randomWriteOverhead - the overhead for a single block write if the
 *  block is not part of a sequence that is written. For magnetic disks,
 *  that would correspond to seek time + rotational latency.
 *
 * Please bear in mind that the implementation of the different methods should
 * take into account that the page size of the data structures required by the
 * physical operator might differ from the default one (e.g. an index page might
 * be 4 times bigger than the default one, correspondingly the readCost and
 * writeCost used in the computeIndexLookupCosts() method should be 4 times
 * larger then the default ones).
 *
 * @author Alexander Alexandrov (alexander.alexandrov@tu-berlin.de)
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */



public class MyCostEstimator implements CostEstimator {





    private long readCost;
    private long writeCost;
    private long randomReadOverhead;
    private long randomWrireOverhead;



    public MyCostEstimator(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead){


        this.readCost = readCost;
        this.writeCost = writeCost;
        this.randomReadOverhead = randomReadOverhead;
        this.randomWrireOverhead = randomWriteOverhead;

    }


    /**
     * Computes the I/O costs for the given table. The cost model assumes a simple
     * sequential scan. The overhead of the random access between the sequential
     * sub-sequences is assumed to be hidden through prefetching. Adjusting the disk
     * head before reading the first block should be taken into account.
     *
     * @param table The descriptor of the table to be scanned.
     * @return The I/O costs (microseconds) for the table scan operation.
     */
    public long computeTableScanCosts(TableDescriptor table){


        PageSize size = table.getSchema().getPageSize();
        int numberOfPages = table.getStatistics().getNumberOfPages();

        return numberOfPages * getReadCost(size) + randomReadOverhead;


    }


    private long getReadCost(PageSize pageSize) {

        return (readCost * pageSize.getNumberOfBytes()) / PageSize.getDefaultPageSize().getNumberOfBytes();
    }


    private long getWriteCost(PageSize pageSize) {

        return (writeCost * pageSize.getNumberOfBytes()) / PageSize.getDefaultPageSize().getNumberOfBytes();
    }

    /**
     * Computes the costs of an index scan. The costs are assumed to be the descend down the
     * B-Tree (random I/O, minus pages from the first two levels in the cache) and then the scan along the leafs,
     * which is assumed to be sequential.
     *
     * @param index The descriptor of the index.
     * @param baseTable The descriptor of the table that is indexed.
     * @param cardinality The result cardinality of the scan.
     * @return The I/O costs (microseconds) for the index scan operation.
     */
    public long computeIndexLookupCosts(IndexDescriptor index, TableDescriptor baseTable, long cardinality){



        long dcost = 0;
        int depth;

        if ((depth = index.getStatistics().getTreeDepth()) > 2)
            dcost = (depth - 2) * getReadCost(index.getSchema().getPageSize()) +  randomReadOverhead;



        long pageNbr;

        if (cardinality == baseTable.getStatistics().getCardinality())
            pageNbr = index.getStatistics().getNumberOfLeafs() + 1;
        else
            pageNbr = (long) Math.ceil(index.getStatistics().getNumberOfLeafs() * 1f *  cardinality / baseTable.getStatistics().getCardinality());

        long scanCost = randomReadOverhead + pageNbr * getReadCost(baseTable.getSchema().getPageSize());





        return dcost + scanCost;


    }


    /**
     * Computes the estimated I/O costs for a sort operation. For simplicity and robustness,
     * the sort is assumed to be always external and two-phase multi-way. That is, all
     * tuples are written in blocks once in a sequence (or rather in multiple sequences that
     * are directly one after the other) and then read again.
     * <p>
     * For simplicity, we assume that the all writes occur directly one after the other, so
     * only one random write overhead should be taken into account for the first block that is
     * written.
     * <p>
     * For the read phase all blocks are read once. The sort code can utilize some form of
     * prefetching on the blocks, but random access is not completely excluded as the algorithm
     * has to jump through the sequences. Therefore, as an ad-hoc approximation for each case,
     * we assume that each block read has an associated random read overhead which accounts to
     * one sixteenth of the random read overhead for the default page size.
     *
     * Hint: use the QueryHeap.getTupleBytes() method to compute the number of bytes required
     * to store a single tuple on the query heap.
     *
     * @param columnsInTuple The columns in the tuples that are sorted. That parameter is used
     *                       to compute the space per tuple and the tuples per block.
     * @param numTuples The number of tuples to be sorted.
     * @return The I/O costs (microseconds) for the sort operation.
     */
    public long computeSortCosts(Column[] columnsInTuple, long numTuples){


        DataType[] types = new DataType[columnsInTuple.length];

        for (int i = 0; i < columnsInTuple.length; i++){
            types[i] = columnsInTuple[i].getDataType();
        }

        int tupleSize =  QueryHeap.getTupleBytes(types);
        PageSize pageSize = QueryHeap.getPageSize();


        long readCost;
        long writeCost;




        long tuples = (long) Math.ceil((pageSize.getNumberOfBytes() - 32) / tupleSize);


        long nbrOfBlocks = (long) Math.max(Math.ceil(numTuples / tuples), 1);



        readCost = nbrOfBlocks * (getReadCost(pageSize) + randomReadOverhead / 16);
        writeCost = getWriteCost(pageSize) * nbrOfBlocks + randomWrireOverhead;

        return readCost + writeCost;
    }


    /**
     * Computes the costs for the FETCH operator.
     * <p>
     * If the FETCH operator receives an input sorted after the RID, it uses a model where no
     * page is assumed to be read twice and some pages are still most likely omitted when each
     * tuple has equal chance to go to each page. The cost of seeking a page goes down with
     * the density of the fetches due to file system and disk cache prefetching even.
     * <p>
     * If the FETCH receives RIDs that are not sorted, then it has to access each page
     * individually. Here, the cache hit ratio increases when the number of tuples to be
     * fetched increases.
     * <p>
     * More specifically, if the RIDs are sorted, the model is the following: assume we have k
     * pages and a cardinality of c for the first tuple we need a fetch. For the second, we
     * fetch with a certain probability a new page, which depends on the number of pages we
     * have seen already. This leas to a recursive formula for the number of accessed pages
     *
     *     <pre>pages(card + 1) = pages(card) + (1 - (pages(card) / totalPages))  [1]</pre>
     *
     * which can be rewritten as
     *
     *     <pre>pages(card + 1) = (totalPages - 1) / totalPages * pages(card) + 1  [2]</pre>
     *
     * Solving recursion [2] (cf. solution of linear differential equation) gives us
     *
     *     <pre>pages(n) = (1/k - 1/(k-k^2)) * k^n + 1/(1-k) with k = (totalPages - 1) / totalPages</pre>
     *
     * Further, the ratio of the pages that are accessed randomly by the fetch operator is this
     * case is approximated by the following formula:
     *
     *     <pre> 1 - (pageAccesses / numTablePages) * (7.0f / 8.0f)</pre>
     * <p>
     * If the FETCH receives RIDs that are not sorted, we assume that each fetch goes to a
     * different (random) page and we have a cache hit ratio H that is given by the formula
     *
     *     <pre> H = log(B, cardinality) </pre>
     *
     * where the log base B is defined as
     *
     *     <pre> B = pow(10 * numOfPages, 1.0 / 0.66) </pre>
     *
     * and the maximal possible value for H is 0.8.
     *
     * @param fetchedTable The table from which to fetch.
     * @param cardinality The number of fetches to do.
     * @param sequential Flag indicating whether the RIDs are sorted or not.
     * @return The I/O costs (microseconds) for the fetch operation.
     */
    public long computeFetchCosts(TableDescriptor fetchedTable, long cardinality, boolean sequential){



        PageSize pageSize = fetchedTable.getSchema().getPageSize();
        int totalPages = fetchedTable.getStatistics().getNumberOfPages();





        if (sequential) {


            float k = 1.0f*(totalPages - 1) / totalPages;


            double nbrPages = Math.floor((1 - Math.pow(k, cardinality))/(1-k));

            double ratio = 1 - (1.0f*nbrPages / totalPages) * (7.0f / 8.0f);



            return (long)Math.floor(nbrPages * (getReadCost(pageSize) + ratio * randomReadOverhead));

        } else {

            double B = Math.pow(10 * totalPages, 1.0 / 0.66);
            double H = Math.min(Math.log(cardinality) / Math.log(B), 0.8);

            return (long) Math.floor(cardinality * (1-H) * (getReadCost(pageSize) + randomReadOverhead));
        }


    }


    /**
     * Computes the I/O cost of a filter operator (const 0 costs).
     *
     * @param pred The predicate that is applied.
     * @param cardinality The number of rows that enter the filter operator.
     * @return The I/O costs (microseconds) for the filter operation.
     */
    public  long computeFilterCost(LocalPredicate pred, long cardinality){



        return 0L;

    }


    /**
     * Computes the costs of a Merge Join. Currently, the merge join is assumed to be
     * free, because it has no I/O itself. All cost is represented in the required
     * sort operators.
     *
     * @return The I/O costs (microseconds) for the merge join operation.
     */
    public long computeMergeJoinCost(){



        return 0L;
    }


    /**
     * Computes the costs of a nested loop join. The cost are directly derived from the
     * operator semantics and assume that the inner child is executed once for each
     * tuple of the outer side.
     *
     * @param outerCardinality The cardinality of the outer side.
     * @param innerOp The inner plan of the join operator.
     * @return The I/O costs (microseconds) for the merge join operation.
     */
    public long computeNestedLoopJoinCost(long outerCardinality, OptimizerPlanOperator innerOp){


        return  outerCardinality * innerOp.getCumulativeCosts();
    }
}
