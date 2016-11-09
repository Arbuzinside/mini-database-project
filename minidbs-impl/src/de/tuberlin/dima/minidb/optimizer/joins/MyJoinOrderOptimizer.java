package de.tuberlin.dima.minidb.optimizer.joins;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.joins.util.JoinOrderOptimizerUtils;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class MyJoinOrderOptimizer implements JoinOrderOptimizer {


    private CardinalityEstimator estimator;

    private HashMap<Integer, HashSet<Integer>> levelPlanIds = new HashMap<>();
    private HashMap<Integer, MyPlan> plans = new HashMap<>();

    public MyJoinOrderOptimizer(CardinalityEstimator estimator) {
        this.estimator = estimator;
    }


    /**
     * Finds the best join order for a query represented as the given graph.
     * <p/>
     * The input is an array of <tt>TableScanPlanOperator</tt>, which is sorted
     * by the <i>Scan-Id</i> of the operator, to establish an internal ordering, which
     * can be exploited to more efficiently implement some algorithms.
     * <p/>
     * Similarly, every <tt>JoinGraphEdge</tt> created such that the table scan with
     * the lower <i>Scan-Id</i> is always on the left side. The array of the edges is
     * sorted primarily after the <i>Scan-Id</i> of its left node and secondarily after
     * the <i>Scan-Id</i> of its right node.
     *
     * @param tables The tables, forming the nodes in the join graph.
     * @param joins  The joins, forming the edges in the join graph.
     * @return The abstract best plan, restricted to the join operators.
     */

    @Override
    public OptimizerPlanOperator findBestJoinOrder(Relation[] relations, JoinGraphEdge[] joins) {

        HashSet<Integer> initialPlanIds = new HashSet<>();


        //init one level relations
        for (int i = 0; i < relations.length; i++) {
            Relation r = relations[i];
            int bitmap = 0b1 << i;

            int relationBitmap = (int) Math.pow(2, r.getID());
            int nextBitmap = 0;

            for (JoinGraphEdge join : joins) {
                if (join.getLeftNode().getID() == r.getID())
                    nextBitmap += Math.pow(2, join.getRightNode().getID());
                else if (join.getRightNode().getID() == r.getID())
                    nextBitmap += Math.pow(2, join.getLeftNode().getID());
            }


            MyPlan plan = new MyPlan(r, bitmap);

            plan.setBitmap(relationBitmap);
            plan.setNextBitmap(nextBitmap);

            plans.put(bitmap, plan);
            initialPlanIds.add(bitmap);
        }

        levelPlanIds.put(0, initialPlanIds);

        int level = 0;
        while (level < relations.length) {

            HashSet<Integer> currentLevelPlans = new HashSet<>();

            int r = 0;
            while (r <= level) {
                if (levelPlanIds.containsKey(level)) {

                    HashSet<Integer> leftPlans = levelPlanIds.get(level);
                    HashSet<Integer> rightPlans = levelPlanIds.get(r);
                    JoinPredicate predicate;

                    for (int leftId : leftPlans) {
                        for (int rightId : rightPlans) {


                            MyPlan left = plans.get(leftId);
                            MyPlan right = plans.get(rightId);

                            Set<Relation> leftRelations = left.getOperator().getInvolvedRelations();
                            Set<Relation> rightRelations = right.getOperator().getInvolvedRelations();


                            JoinPredicateConjunct joinPred;
                            joinPred = new JoinPredicateConjunct();
                            boolean join = false;


                            for (JoinGraphEdge e : joins)
                                if (leftRelations.contains(e.getLeftNode()) && rightRelations.contains(e.getRightNode())) {
                                    joinPred.addJoinPredicate(e.getJoinPredicate());
                                    join = true;
                                } else {
                                    if (!leftRelations.contains(e.getRightNode()) || !rightRelations.contains(e.getLeftNode())) {
                                        continue;
                                    }
                                    joinPred.addJoinPredicate(e.getJoinPredicate().createSideSwitchedCopy());
                                    join = true;
                                }


                            if (join) {

                                predicate = JoinOrderOptimizerUtils.filterTwinPredicates(joinPred);


                                //create new plan
                                AbstractJoinPlanOperator plan = new AbstractJoinPlanOperator(left.getOperator(), right.getOperator(), predicate);
                                estimator.estimateJoinCardinality(plan);
                                plan.setOperatorCosts(left.getOperator().getCumulativeCosts() + right.getOperator().getCumulativeCosts());
                                plan.setCumulativeCosts(plan.getOutputCardinality() + plan.getOperatorCosts());


                                int bitmap = leftId | rightId;
                                int nextBitmap = (left.getNextBitmap() | right.getNextBitmap()) & (~bitmap);
                                MyPlan newPlan = new MyPlan(plan, bitmap);
                                newPlan.setNextBitmap(nextBitmap);


                                if (!plans.containsKey(bitmap)) {
                                    currentLevelPlans.add(bitmap);
                                    plans.put(bitmap, newPlan);
                                } else {

                                    long prevCost = plans.get(bitmap).getOperator().getCumulativeCosts();

                                    if (plan.getCumulativeCosts() < prevCost) {
                                        currentLevelPlans.add(bitmap);
                                        plans.put(bitmap, newPlan);
                                    }
                                }
                            }

                        }
                    }
                }
                r++;
            }
            levelPlanIds.put(level + 1, currentLevelPlans);
            level++;
        }

        return getBestPlan(relations.length);


    }


    private OptimizerPlanOperator getBestPlan(int length) {


        OptimizerPlanOperator bestPlan = null;

        for (MyPlan plan : plans.values()) {
            if (plan.getOperator().getInvolvedRelations().size() == length) {
                if (bestPlan == null)
                    bestPlan = plan.getOperator();

                if (plan.getOperator().getCumulativeCosts() < bestPlan.getCumulativeCosts())
                    bestPlan = plan.getOperator();

            }
        }
        
        return bestPlan;
        
    }
}