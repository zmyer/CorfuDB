package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

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
 * First, forget about nested transactions for now, and neglect the contexts
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

    // the last position in the relevant TX stack which has been applied
    PositionTracker lastPos = new PositionTracker();

    // the list of SMREntries which have been applied optimistically;
    // it is used for optimistic rollback
    Stack<SMREntry> optUpdates = new Stack<>();

    // the ID of the TX stack which this stream applied entries from
    UUID TXid;

    List<AbstractTransactionalContext> contexts;

    int currentContext = 0;

    // TODO add comment
    long currentContextPos;

    // TODO add comment
    long writePos;

    // the specific stream-id for which this SMRstream wraps the write-set
    final UUID id;

    public WriteSetSMRStream(List<AbstractTransactionalContext> contexts,
                             UUID id) {
        this.contexts = contexts;
        this.id = id;

        reset();

        this.TXid = contexts.get(0).getTransactionID();
        log.debug("INIT[{}]", this);
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
        boolean b1, b2;

        b1 = TransactionalContext.isPositionTrackerAtTail(id,
                lastPos);
        b2 = contexts.get(currentContext)
                .equals(TransactionalContext.getCurrentContext());
        if (b1 != b2)
            log.warn("TAIL[{}] position tracker {} != current {}", this, b1,
                    b2);
        return b2;
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
            // recalculate the pos based on the write pointer
            // TODO add explanation, code below very confusing!
            long readPos = Address.maxNonAddress();
            for (int i = 0; i < contexts.size(); i++) {
                readPos += contexts.get(i).getWriteSetEntryList(id).size();
                if (readPos >= writePos) {
                    currentContextPos = contexts.get(i).getWriteSetEntryList(id).size()
                                        - (writePos - readPos) - 1;
                }
            }
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

                // get the next entry
                // keep a local reference to it for optimistic-rollback purposes
                /*
                SMREntry nextEntry;
                if ((nextEntry = TransactionalContext.next(id, lastPos)) != null)
                    optUpdates.push(nextEntry);
                else
                    log.warn("SMRstrea[{}] NEXT no next entry", id);
                if (lastPos.get() != writePos)
                    log.warn("SMRstream[{}] NEXT writePos={} != lastPos={}", id, writePos, lastPos.get());
                log.debug("SMRstream[{}] NEXT writePos={} lastPos={}", id, writePos, lastPos.get());
                */
            }
            if (writeSet.size() > 0) {
                currentContext = i;
                currentContextPos = writeSet.size() - 1;
            }
        }

        /**/
        SMREntry entry;
        List<SMREntry> entryLinkedList = new LinkedList<>();
        while ((entry = TransactionalContext.next(id, lastPos)) != null) {
            optUpdates.push(entry);
            entryLinkedList.add(entry);
        }
        if (lastPos.getPos() != writePos)
            log.warn("NEXT[{}] writePos={} != lastPos={}", this,
                    writePos, lastPos.getPos());
        log.debug("NEXT[{}] writePos={} lastPos={}", this, writePos,
                lastPos.getPos());
        /**/

        //return entryList;
        return entryLinkedList;
    }

    @Override
    public List<SMREntry> current() {
        if (Address.nonAddress(writePos)) {
            return null;
        }
        if (Address.nonAddress(currentContextPos))
            currentContextPos = -1;
        return Collections.singletonList(contexts
                .get(currentContext)
                .getWriteSetEntryList(id)
                .get((int)(currentContextPos)));
    }

    @Override
    public List<SMREntry> previous() {
        writePos--;

        TransactionalContext.prev(id, lastPos); // new

        if (writePos <= Address.maxNonAddress()) {
            writePos = Address.maxNonAddress();
            return null;
        }

        currentContextPos--;
        // Pop the context if we're at the beginning of it
        if (currentContextPos <= Address.maxNonAddress()) {
            do {
                if (currentContext == 0) {
                    throw new RuntimeException("Attempted to pop first context (pos=" + pos() + ")");
                } else {
                    currentContext--;
                }
            } while (contexts
                    .get(currentContext)
                    .getWriteSetEntrySize(id) == 0);
            currentContextPos = contexts
                    .get(currentContext)
                    .getWriteSetEntrySize(id)-1 ;
        }

        // new
        optUpdates.pop();
        log.debug("PREV[{}] lastPos={}", this, lastPos.getPos());
        if (writePos != lastPos.getPos())
            log.warn("PREV[{}]  writePos={} lastPos={}",
                    this, writePos, lastPos.getPos());

        SMREntry ent = optUpdates.peek();
        if (ent != current().get(0))
            log.warn("PREV[{}] ent != current", id, ent, current());

//        return current();
        return Collections.singletonList(ent);
    }

    @Override
    public long pos() {
        log.debug("POS[{}] writePos={} lastPos={}",this, writePos, lastPos
                .getPos());
        return lastPos.getPos();
        //return writePos;
    }

    @Override
    public void reset() {
        lastPos.setPos(Address.NEVER_READ);
        writePos = Address.maxNonAddress();
        currentContext = 0;
        currentContextPos = Address.maxNonAddress();
    }

    @Override
    public void seek(long globalAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<SMREntry> stream() {
        return streamUpTo(Address.MAX);
    }

    @Override
    public Stream<SMREntry> streamUpTo(long maxGlobal) {
        return remainingUpTo(maxGlobal)
                .stream();
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
        return Utils.toReadableID(getID()) + "," +
                "TX=" + Utils.toReadableID(TXid) ;
    }
}
