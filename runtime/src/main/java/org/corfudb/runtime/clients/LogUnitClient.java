package org.corfudb.runtime.clients;

import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ReplexOverwriteException;
import org.corfudb.util.serializer.Serializers;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


/**
 * A client to a LogUnit.
 * <p>
 * This class provides access to operations on a remote log unit.
 * Created by mwei on 12/10/15.
 */
public class LogUnitClient extends AbstractEpochedClient {

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler<CorfuMsg,CorfuMsgType> msgHandler =
            new ClientMsgHandler<CorfuMsg,CorfuMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            ClientHandler.class, ClientHandler::type);

    public LogUnitClient(IRequestClientRouter<CorfuMsg, CorfuMsgType> router,
                           CorfuRuntime runtime) {
        super(router, runtime);
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        The address to write to.
     * @param streams        The streams, if any, that this write belongs to.
     * @param rank           The rank of this write (used for quorum replication).
     * @param writeObject    The object, pre-serialization, to write.
     * @param backpointerMap The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            Object writeObject, Map<UUID, Long> backpointerMap) {
        ByteBuf payload = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize(writeObject, payload);
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, null, payload);
        wr.setStreams(streams);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        wr.setGlobalAddress(address);
        return sendMessageAndGetResponse(CorfuMsgType.WRITE.payloadMsg(wr))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.WRITE_OK_RESPONSE);
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address        The address to write to.
     * @param streams        The streams, if any, that this write belongs to.
     * @param rank           The rank of this write (used for quorum replication).
     * @param buffer         The object, post-serialization, to write.
     * @param backpointerMap The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            ByteBuf buffer, Map<UUID, Long> backpointerMap) {
        WriteRequest wr = new WriteRequest(WriteMode.NORMAL, null, buffer);
        wr.setStreams(streams);
        wr.setRank(rank);
        wr.setBackpointerMap(backpointerMap);
        wr.setGlobalAddress(address);
        return sendMessageAndGetResponse(CorfuMsgType.WRITE.payloadMsg(wr))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.WRITE_OK_RESPONSE);
    }

    public CompletableFuture<Boolean> writeStream(long address, Map<UUID, Long> streamAddresses,
                                                  Object object) {
        ByteBuf payload = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize(object, payload);
        return writeStream(address, streamAddresses, payload);
    }

    public CompletableFuture<Boolean> writeStream(long address, Map<UUID, Long> streamAddresses,
                                                  ByteBuf buffer) {
        WriteRequest wr = new WriteRequest(WriteMode.REPLEX_STREAM, streamAddresses, buffer);
        wr.setLogicalAddresses(streamAddresses);
        wr.setGlobalAddress(address);
        return sendMessageAndGetResponse(CorfuMsgType.WRITE.payloadMsg(wr))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.WRITE_OK_RESPONSE);
    }

    public CompletableFuture<Boolean> writeCommit(Map<UUID, Long> streams, long address, boolean commit) {
        CommitRequest wr = new CommitRequest(streams, address, commit);
        return sendMessageAndGetResponse(CorfuMsgType.COMMIT.payloadMsg(wr))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.WRITE_OK_RESPONSE);
    }

    /**
     * Asynchronously read from the logging unit.
     *
     * @param address The address to read from.
     * @return A CompletableFuture which will complete with a ReadResult once the read
     * completes.
     */
    public CompletableFuture<ReadResponse> read(long address) {
        return sendMessageAndGetResponse(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(address)),
                new TypeToken<CorfuPayloadMsg<ReadResponse>>() {})
                .thenApply(x -> x.getPayload());
    }

    public CompletableFuture<ReadResponse> read(UUID stream, Range<Long> offsetRange) {
        return sendMessageAndGetResponse(
                CorfuMsgType.READ_REQUEST.payloadMsg(new ReadRequest(offsetRange, stream)),
                new TypeToken<CorfuPayloadMsg<ReadResponse>>() {})
                .thenApply(x -> x.getPayload());
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param stream The stream to trim.
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    public void trim(UUID stream, long prefix) {
        sendMessage(CorfuMsgType.TRIM.payloadMsg(new TrimRequest(stream, prefix)));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    public CompletableFuture<Boolean> fillHole(long address) {
        return sendMessageAndGetResponse(
                CorfuMsgType.FILL_HOLE.payloadMsg(new FillHoleRequest(null, address)))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    public CompletableFuture<Boolean> fillHole(UUID streamID, long address) {
        return sendMessageAndGetResponse(
                CorfuMsgType.FILL_HOLE.payloadMsg(new FillHoleRequest(streamID, address)))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }


    /**
     * Force the garbage collector to begin garbage collection.
     */
    public void forceGC() {
        sendMessage(CorfuMsgType.FORCE_GC.msg());
    }

    /**
     * Force the compactor to recalculate the contiguous tail.
     */
    public void forceCompact() {
        sendMessage(CorfuMsgType.FORCE_COMPACT.msg());
    }

    /**
     * Change the default garbage collection interval.
     *
     * @param millis The new garbage collection interval, in milliseconds.
     */
    public void setGCInterval(long millis) {
        sendMessage(CorfuMsgType.GC_INTERVAL.payloadMsg(millis));
    }


}
