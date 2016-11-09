package de.tuberlin.dima.minidb.optimizer.joins;

import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;

/**
 * Created by arbuzinside on 13.1.2016.
 */
public class MyPlan {


    private OptimizerPlanOperator operator;


    private int bitmap;
    private int nextBitmap;

    public MyPlan(OptimizerPlanOperator operator) {


        this.setOperator(operator);

    }

    public MyPlan(OptimizerPlanOperator operator, int id) {

        this.setBitmap(id);
        this.setOperator(operator);

    }





    public OptimizerPlanOperator getOperator() {
        return operator;
    }

    public void setOperator(OptimizerPlanOperator operator) {
        this.operator = operator;
    }

    public int getBitmap() {
        return bitmap;
    }

    public void setBitmap(int bitmap) {
        this.bitmap = bitmap;
    }

    public int getNextBitmap() {
        return nextBitmap;
    }

    public void setNextBitmap(int nextBitmap) {
        this.nextBitmap = nextBitmap;
    }
}
