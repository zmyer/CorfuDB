package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** A class which allows access to transactional contexts, which manage
 * transactions. The static methods of this class provide access to the
 * thread's transaction stack, which is a stack of transaction contexts
 * active for a particular thread.
 *
 * Created by mwei on 1/11/16.
 */
@Slf4j
public class TransactionalContext {

    /** A thread local stack containing all transaction contexts
     * for a given thread.
     */
    private static final ThreadLocal<LinkedList<AbstractTransactionalContext>>
            threadTransactionStack = ThreadLocal.withInitial(
            LinkedList<AbstractTransactionalContext>::new);

    /** Whether or not the current thread is in a nested transaction.
     *
     * @return  True, if the current thread is in a nested transaction.
     */
    public static boolean isInNestedTransaction() {return threadTransactionStack.get().size() > 1;}

    /**
     * Returns the transaction stack for the calling thread.
     *
     * @return The transaction stack for the calling thread.
     */
    public static LinkedList<AbstractTransactionalContext> getTransactionStack() {
        return threadTransactionStack.get();
    }

    /**
     * Returns the current transactional context for the calling thread.
     *
     * @return The current transactional context for the calling thread.
     */
    public static AbstractTransactionalContext getCurrentContext() {
        return getTransactionStack().peekFirst();
    }

    /**
     * Returns the last transactional context (parent/root) for the calling thread.
     *
     * @return The last transactional context for the calling thread.
     */
    public static AbstractTransactionalContext getRootContext() {
        return getTransactionStack().peekLast();
    }

    /**
     * Returns whether or not the calling thread is in a transaction.
     *
     * @return True, if the calling thread is in a transaction.
     * False otherwise.
     */
    public static boolean isInTransaction() {
        return getTransactionStack().peekFirst() != null;
    }

    /** Add a new transactional context to the thread's transaction stack.
     *
     * @param context   The context to add to the transaction stack.
     * @return          The context which was added to the transaction stack.
     */
    public static AbstractTransactionalContext getNewTXContext(AbstractTransactionalContext context) {
        log.debug("TX begin[{}]", context);
        getTransactionStack().addFirst(context);
        return context;
    }

    /** Remove the most recent transaction context from the transaction stack.
     *
     * @return          The context which was removed from the transaction stack.
     */
    public static AbstractTransactionalContext foldTXContext() {
        log.trace("TX-context[{}] remove context", getCurrentContext());
        AbstractTransactionalContext r = getTransactionStack().pollFirst();
        if (getTransactionStack().isEmpty()) {
            synchronized (getTransactionStack())
            {
                getTransactionStack().notifyAll();
            }
        }
        return r;
    }

    /**
     * This method implements streaming functionality over the TX write-set.
     *
     * @param streamID the specific stream we iterate over
     * @param lastPos the last stream-index which has been accessed
     * @return the next SMREntry in the stream, if exists; null if none exists,
     *          and also adjust the stream index to the next entry.
     */
    public static SMREntry
    next(UUID streamID, AtomicReference<Long> lastPos) {


        Iterator<AbstractTransactionalContext> iterator = getTransactionStack().descendingIterator();
        AbstractTransactionalContext ctxt = null;

        long pos = 0, tempPos = lastPos.get()+1;
        boolean hasNext = false;

        while (iterator.hasNext()) {
            ctxt = iterator.next();

            if (pos + ctxt.getWriteSetEntrySize(streamID) > tempPos) {
                // we reached the context that holds the next position
                hasNext = true;
                break;
            } else {
                // accumulate the TX stack write-set sizes into tempPos
                pos += ctxt.getWriteSetEntrySize(streamID);
            }
        }
        if (!hasNext) {
            // todo: check that we have exactly lastPos+1 entries in the stream?
            return null;
        }

        // increment the position
        lastPos.set(tempPos);

        // get the relative position within the TX cotext
        tempPos -= pos;

        // retrieve the SMR entry
        return ctxt.getWriteSetEntryList(streamID).get((int)tempPos);
    }


        /**
         * Get the transaction stack as a list.
         * @return  The transaction stack as a list.
         */
    public static List<AbstractTransactionalContext> getTransactionStackAsList() {
        List<AbstractTransactionalContext> listReverse =
                getTransactionStack().stream().collect(Collectors.toList());
        Collections.reverse(listReverse);
        return listReverse;
    }
}
