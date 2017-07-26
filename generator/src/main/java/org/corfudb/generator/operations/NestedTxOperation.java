package org.corfudb.generator.operations;

import org.corfudb.generator.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

/**
 * Created by rmichoud on 7/26/17.
 */
public class NestedTxOperation extends Operation {

    private final int maxNest = 20;
    public NestedTxOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        state.startOptimisticTx();



        int numNested = state.getOperationCount().sample(1).get(0);
        int nestedTxToStop = numNested;
        for (int i = 0; i < numNested && i < maxNest; i++) {
            try{
            state.startOptimisticTx();
            int numOperations = state.getOperationCount().sample(1).get(0);
            List<Operation> operations = state.getOperations().sample(numOperations);

            System.out.println("Start nested Tx of " + numOperations + "operations");
            System.out.println("Nesting " + i);
            for (int x = 0; x < operations.size(); x++) {
                if (operations.get(x) instanceof OptimisticTxOperation ||
                        operations.get(x) instanceof SnapshotTxOperation
            || operations.get(x) instanceof NestedTxOperation) {
                    continue;
                }
                operations.get(x).execute();
            }
            } catch (TransactionAbortedException tae) {
                tae.printStackTrace();
                System.out.println("Nesting: " + i);
                nestedTxToStop--;
            }
        }

        for (int i = 0; i < nestedTxToStop; i++) {
            state.stopOptimisticTx();
        }

        state.stopOptimisticTx();
    }
}
