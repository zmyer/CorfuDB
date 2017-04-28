package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by mwei on 3/13/17.
 *
 * SMRStreamAdapter wraps an optimistic transaction execution context, per
 * object, with an SMRStream API.
 *
 * The main purpose of wrapping the write-set of optimistic transactions as an
 * SMRStream is to provide the abstraction of a stream of SMREntries. The
 * SMRStream maintains for us a position in the sequence. We can consume it
 * in a forward direction, and scroll back to previously read entries.
 *
 * First, forget about nested transactions for now, and neglected the contexts
 * stack; that is, assume the stack has size 1.
 *
 * A reminder from AbstractTransactionalContext about the write-set of a
 * transaction:
 * * A write-set is a key component of a transaction.
 * * We collect the write-set as a map, organized by streams.
 * * For each stream, we record a pair:
 * *  - a set of conflict-parameters modified by this transaction on the
 * *  stream,
 * *  - a list of SMR updates by this transcation on the stream.
 * *
 *
 * The implementation of the current() method looks at the write-set, picks
 * the list of SMRentries corresponding to the current object id, and returns
 * the entry in the list corredponding the the current SMRStream position.
 *
 * previous() decrements the current SMRStream position and returns the entry
 * corresponding to it.
 *
 * RemainingUpTo() returns a list of entries.
 *
 * WriteSetSMRStream does not support the full API - neither append nor seek are
 * supported.
 *
 * Enter nested transactions.
 *
 * WriteSetSMRStream maintains the abstractions also across nested transactions.
 * It supports navigating forward/backward across the SMREntries in the entire transcation stack.
 *
 */
@Slf4j
public class WriteSetSMRStream implements ISMRStream {

    List<AbstractTransactionalContext> contexts;

    int currentContext = 0;

    long currentContextPos;

    long writePos;

    // the specific stream-id for which this SMRstream wraps the write-set
    final UUID id;

    public WriteSetSMRStream(List<AbstractTransactionalContext> contexts,
                             UUID id) {
        this.contexts = contexts;
        this.id = id;
        reset();
    }

    /** Return whether stream current transaction is the thread current transaction.
     *
     * This is validated by checking whether the current context
     * for this stream is the same as the current context for this thread.
     *
     * @return  True, if the stream current context is the thread current context.
     *          False otherwise.
     */
    public boolean isStreamCurrentContextThreadCurrentContext() {
        return contexts.get(currentContext)
                .equals(TransactionalContext.getCurrentContext());
    }

    /** Return whether we are the stream for this current thread
     *
     * This is validated by checking whether the root context
     * for this stream is the same as the root context for this thread.
     *
     * @return  True, if the thread owns the optimistic stream
     *          False otherwise.
     */
    public boolean isStreamForThisThread() {
        return contexts.get(0)
                .equals(TransactionalContext.getRootContext());
    }

    void mergeTransaction() {
        contexts.remove(contexts.size()-1);
        if (currentContext == contexts.size()) {

            // recalculate the context-pos based on the writePos
            //
            // we need to find the relative context-position within the
            // newly-popped context stack.
            //
            // For example,
            // if there are 5 contexts,
            // each has write-set of size 10,
            // and writepos = 45
            // then the relative context-position of the last context is 5
            // (that is, we subtract 40 from write-pos).
            //

            long contextStackPos = 0L;
            for (int i = 0; i < contexts.size(); i++) {
                // this loop stops when we get to the right context
                // TODO: assert that i == contexts.size()-1??
                if (contextStackPos + contexts.get(i).getWriteSetEntryList(id).size
                        () >= writePos)
                    break;
            }

            // now, simply take writePos modulo the sum of the context-stack
            currentContextPos = writePos - contextStackPos;

            currentContext--;

        }
    }

    @Override
    public List<SMREntry> remainingUpTo(long maxGlobal) {
        // Check for any new contexts
        if (TransactionalContext.getTransactionStack().size() >
                contexts.size()) {
            contexts = TransactionalContext.getTransactionStackAsList();
        } else if (TransactionalContext.getTransactionStack().size() <
                contexts.size()) {
            mergeTransaction();
        }
        List<SMREntry> entryList = new LinkedList<>();


        for (int i = currentContext; i < contexts.size(); i++) {
            final List<SMREntry> writeSet = contexts.get(i)
                    .getWriteSetEntryList(id);
            long readContextStart = i == currentContext ? currentContextPos + 1: 0;
            for (long j = readContextStart; j < writeSet.size(); j++) {
                entryList.add(writeSet.get((int) j));
                writePos++;
            }
            if (writeSet.size() > 0) {
                currentContext = i;
                currentContextPos = writeSet.size() - 1;
            }
        }
        return entryList;
    }

    @Override
    public List<SMREntry> current() {
        if (writePos == Address.NEVER_READ) {
            return null;
        }
        return Collections.singletonList(contexts.get(currentContext)
                .getWriteSet().get(id)
                .getValue().get((int)(currentContextPos)));
    }

    @Override
    public List<SMREntry> previous() {

        if (Address.nonAddress(writePos))
            return null;

        // check if we already back-stepped beyond the start point;
        if (writePos < Address.getMinAddress())
            return null;

        writePos--;

        // Pop the context if we're at the beginning of it
        if (Address.isMinAddress(currentContextPos)) {
            if (currentContext == 0) {
                currentContextPos = Address.NON_ADDRESS;
                throw new RuntimeException("Attempted to pop first context (pos=" + pos() + ")");
            }
            else {
                currentContext--;
                currentContextPos = contexts.get(currentContext)
                        .getWriteSet().get(id).getValue().size() - 1;
            }
        }
        return current();
    }

    @Override
    public long pos() {
        return writePos;
    }

    @Override
    public void reset() {
        writePos = Address.NON_ADDRESS;
        currentContext = 0;
        currentContextPos = Address.NON_ADDRESS;
    }

    @Override
    public void seek(long globalAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long append(SMREntry entry,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID getID() {
        return id;
    }

    @Override
    public String toString() {
        return "WSSMRStream[" + Utils.toReadableID(getID()) +"]";
    }
}
