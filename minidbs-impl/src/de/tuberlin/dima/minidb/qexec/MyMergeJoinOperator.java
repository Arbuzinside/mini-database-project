package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataTuple;
import java.util.ArrayList;

public class MyMergeJoinOperator implements MergeJoinOperator {

    private PhysicalPlanOperator leftChild;
    private PhysicalPlanOperator rightChild;
    private int[] leftJoinColumns;
    private int[] rightJoinColumns;
    private int[] columnMapLeftTuple;
    private int[] columnMapRightTuple;
    private ArrayList<DataTuple> rightList;
    private ArrayList<DataTuple> leftList;
    private int pointerRL;
    private int pointerLL;
    private boolean isRight = false;
    private boolean isLeft = false;
    private int flag;

    
    
    public MyMergeJoinOperator(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns,
                               int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {

            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.leftJoinColumns = leftJoinColumns;
            this.rightJoinColumns = rightJoinColumns;
            this.columnMapLeftTuple = columnMapLeftTuple;
            this.columnMapRightTuple = columnMapRightTuple;
            this.rightList = new ArrayList();
            this.leftList = new ArrayList();
            this.pointerRL = 0;
            this.pointerLL = 0;

    }

    public void open(DataTuple correlatedTuple) throws QueryExecutionException {
        leftChild.open(correlatedTuple);
        rightChild.open(correlatedTuple);
        rightList.clear();
        leftList.clear();
        isRight = false;
        isLeft = false;
        flag = 1;
    }

    public DataTuple next() throws QueryExecutionException {

            while(true) {
                DataTuple rightTuple;
                DataTuple leftTuple;
                while(flag != 1) {
                    if (leftList.isEmpty() || pointerRL == rightList.size()) {
                        pointerRL = 0;
                        if((leftTuple = rightChild.next()) == null) {
                            isLeft = true;
                            if(isRight) {
                                return null;
                            }
                            rightList.clear();
                            flag = 1;
                            continue;
                        }
                        leftList.add(leftTuple);
                        if(!rightList.isEmpty() && compareTo(rightList.get(rightList.size() - 1), leftTuple, leftJoinColumns, rightJoinColumns) < 0) {
                            if(isRight) {
                                return null;
                            }

                            flag = 1;
                        }
                    } else {
                        leftTuple = leftList.get(leftList.size() - 1);
                    }

                    if(!rightList.isEmpty()) {
                        switch(compareTo(rightTuple = rightList.get(pointerRL++), leftTuple, leftJoinColumns, rightJoinColumns)) {
                            case -1:
                                rightList.remove(rightTuple);
                                pointerRL--;
                                break;
                            case 0:
                                if(pointerRL == rightList.size()) {
                                    flag = 1;
                                    pointerLL = leftList.size();
                                }
                                return formatTuple(rightTuple, leftTuple);
                            case 1:
                                leftList.clear();
                        }
                    } else {
                        if(isRight) {
                            return null;
                        }
                        flag = 1;
                    }
                }

                if(!rightList.isEmpty() && pointerLL != leftList.size()) {
                    rightTuple = rightList.get(rightList.size() - 1);
                } else {
                    pointerLL = 0;
                    if((rightTuple = leftChild.next()) == null) {
                        isRight = true;
                        if(isLeft) {
                            return null;
                        }

                        leftList.clear();
                        flag = 2;
                        continue;
                    }

                    rightList.add(rightTuple);
                    if(!leftList.isEmpty() && compareTo(rightTuple, leftList.get(leftList.size() - 1), leftJoinColumns, rightJoinColumns) > 0) {
                        if(isLeft) return null;
                        flag = 2;
                    }
                }

                if(!leftList.isEmpty()) {
                    leftTuple = leftList.get(pointerLL++);
                    switch(compareTo(rightTuple, leftTuple, leftJoinColumns, rightJoinColumns)) {
                        case -1:
                            rightList.clear();
                            break;
                        case 0:
                            if(pointerLL == leftList.size()) {
                                flag = 2;
                                pointerRL = rightList.size();
                            }
                            return formatTuple(rightTuple, leftTuple);
                        case 1:
                            leftList.remove(leftTuple);
                            pointerLL--;
                    }
                } else {
                    if(isLeft) {
                        return null;
                    }
                    flag = 2;
                }
            }

    }

    public void close() throws QueryExecutionException {
        leftList.clear();
        rightList.clear();
        pointerRL = 0;
        pointerLL = 0;
    }

    private int compareTo(DataTuple rightTuple, DataTuple leftTuple, int[] leftJoinColumns, int[] rightJoinColumns) {
        if(rightTuple == null) {
            return -1;
        } else if(leftTuple == null) {
            return 1;
        } else {
            for(int i = 0; i < leftJoinColumns.length; i++) {
                int res;
                if((res = rightTuple.getField(leftJoinColumns[i]).compareTo(leftTuple.getField(rightJoinColumns[i]))) != 0) {
                    return res;
                }
            }

            return 0;
        }
    }

    private DataTuple formatTuple(DataTuple rightTuple, DataTuple leftTuple) {
        DataTuple output = new DataTuple(columnMapLeftTuple.length);

        for(int i = 0; i < columnMapLeftTuple.length; ++i) {
            int pos;
            if((pos = columnMapLeftTuple[i]) != -1) {
                output.assignDataField(rightTuple.getField(pos), i);
            } else if((pos = columnMapRightTuple[i]) != -1) {
                output.assignDataField(leftTuple.getField(pos), i);
            }
        }

        return output;
    }
}