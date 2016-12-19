package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.util.function.Function;

/**
 * Created by mwei on 8/8/16.
 */
public enum CorfuMsgType implements IRespondableMsgType<CorfuMsg> {
    // Base Messages
    PING(0, TypeToken.of(CorfuMsg.class)),
    PONG_RESPONSE(1, TypeToken.of(CorfuMsg.class)),
    RESET(2, TypeToken.of(CorfuMsg.class)),
    SEAL_EPOCH(3, new TypeToken<CorfuPayloadMsg<Long>>() {}),
    ACK_RESPONSE(4, TypeToken.of(CorfuMsg.class)),
    WRONG_EPOCH_ERROR(5, new TypeToken<CorfuPayloadMsg<Long>>() {}),
    NACK_ERROR(6, TypeToken.of(CorfuMsg.class)),
    VERSION_REQUEST(7, TypeToken.of(CorfuMsg.class)),
    VERSION_RESPONSE(8, new TypeToken<JSONPayloadMsg<VersionInfo>>() {}),

    // Layout Messages
    LAYOUT_REQUEST(10, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    LAYOUT_RESPONSE(11, TypeToken.of(LayoutMsg.class)),
    LAYOUT_PREPARE(12, new TypeToken<CorfuPayloadMsg<LayoutPrepareRequest>>(){}),
    LAYOUT_PREPARE_REJECT_ERROR(13, new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>(){},
            x ->
                    new OutrankedException(((CorfuPayloadMsg<LayoutProposeResponse>)x).getPayload().getRank())),
    LAYOUT_PROPOSE(14, new TypeToken<CorfuPayloadMsg<LayoutProposeRequest>>(){}),
    LAYOUT_PROPOSE_REJECT_ERROR(15, new TypeToken<CorfuPayloadMsg<LayoutProposeResponse>>(){},
            x ->
                    new OutrankedException(((CorfuPayloadMsg<LayoutProposeResponse>)x).getPayload().getRank())),
    LAYOUT_COMMITTED(16, new TypeToken<CorfuPayloadMsg<LayoutCommittedRequest>>(){}),
    LAYOUT_QUERY(17, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    LAYOUT_BOOTSTRAP(18, new TypeToken<CorfuPayloadMsg<LayoutBootstrapRequest>>(){}),
    LAYOUT_NOBOOTSTRAP_ERROR(19, TypeToken.of(CorfuMsg.class), x -> new NoBootstrapException()),

    // Sequencer Messages
    TOKEN_REQUEST(20, new TypeToken<CorfuPayloadMsg<TokenRequest>>(){}),
    TOKEN_RESPONSE(21, new TypeToken<CorfuPayloadMsg<TokenResponse>>(){}),

    // Logging Unit Messages
    WRITE(30, new TypeToken<CorfuPayloadMsg<WriteRequest>>() {}),
    READ_REQUEST(31, new TypeToken<CorfuPayloadMsg<ReadRequest>>() {}),
    READ_RESPONSE(32, new TypeToken<CorfuPayloadMsg<ReadResponse>>() {}),
    TRIM(33, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}),
    FILL_HOLE(34, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}),
    FORCE_GC(35, TypeToken.of(CorfuMsg.class)),
    GC_INTERVAL(36, new TypeToken<CorfuPayloadMsg<Long>>() {}),
    FORCE_COMPACT(37, TypeToken.of(CorfuMsg.class)),
    COMMIT(40, new TypeToken<CorfuPayloadMsg<CommitRequest>>() {}),

    WRITE_OK_RESPONSE(50, TypeToken.of(CorfuMsg.class)),
    TRIMMED_ERROR(51, TypeToken.of(CorfuMsg.class), x -> new OverwriteException()),
    OVERWRITE_ERROR(52, TypeToken.of(CorfuMsg.class), x-> new OverwriteException()),
    OOS_ERROR(53, TypeToken.of(CorfuMsg.class), x -> new OutOfSpaceException()),
    RANK_ERROR(54, TypeToken.of(CorfuMsg.class), x -> new OutrankedException(0L)),
    NOENTRY_ERROR(55, TypeToken.of(CorfuMsg.class)),
    REPLEX_OVERWRITE_ERROR(56, TypeToken.of(CorfuMsg.class), x -> new OverwriteException()),
    DATA_CORRUPTION_ERROR(57, new TypeToken<CorfuPayloadMsg<ReadResponse>>() {},
        x -> new DataCorruptionException()),

    // EXTRA CODES
    LAYOUT_ALREADY_BOOTSTRAP_ERROR(60, TypeToken.of(CorfuMsg.class), x ->
            new AlreadyBootstrappedException()),
    LAYOUT_PREPARE_ACK_RESPONSE(61, new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>(){}),

    // Management Codes
    MANAGEMENT_BOOTSTRAP(62, new TypeToken<CorfuPayloadMsg<Layout>>(){}),
    MANAGEMENT_FAILURE_DETECTED(63, new TypeToken<CorfuPayloadMsg<FailureDetectorMsg>>(){}),
    MANAGEMENT_NOBOOTSTRAP_ERROR(64, TypeToken.of(CorfuMsg.class), x ->
            new NoBootstrapException()),
    MANAGEMENT_ALREADY_BOOTSTRAP_ERROR(65, TypeToken.of(CorfuMsg.class), x ->
        new AlreadyBootstrappedException());

    public final int type;
    public final TypeToken<? extends CorfuMsg> messageType;
    @Getter
    public final Function<CorfuMsg, Exception> exceptionGenerator;
    //public final Class<? extends AbstractServer> handler;

    CorfuMsgType(int type, TypeToken<? extends CorfuMsg> messageType) {
        this.type = type;
        this.messageType = messageType;
        exceptionGenerator = null;
    }

    CorfuMsgType(int type, TypeToken<? extends CorfuMsg> messageType,
                 Function<CorfuMsg, Exception> exceptionGenerator) {
        this.type = type;
        this.messageType = messageType;
        this.exceptionGenerator = exceptionGenerator;
    }

    public <T> CorfuPayloadMsg<T> payloadMsg(T payload) {
        // todo:: maybe some typechecking here (performance impact?)
        return new CorfuPayloadMsg<T>(this, payload);
    }

    @Getter(lazy=true)
    private final boolean response = calculateIsResponse();

    private boolean calculateIsResponse() {
        return this.toString().endsWith("_RESPONSE");
    }

    @Getter(lazy=true)
    private final boolean error = calculateIsError();

    private boolean calculateIsError() {
        return this.toString().endsWith("_ERROR");
    }

    public CorfuMsg msg() {
        return new CorfuMsg(this);
    }


    @FunctionalInterface
    interface MessageConstructor<T> {
        T construct();
    }

    @Getter(lazy=true)
    private final MessageConstructor<? extends CorfuMsg> constructor = resolveConstructor();

    public byte asByte() {
        return (byte) type;
    }

    /** A lookup representing the context we'll use to do lookups. */
    private static java.lang.invoke.MethodHandles.Lookup lookup = MethodHandles.lookup();

    /** Generate a lambda pointing to the constructor for this message type. */
    @SuppressWarnings("unchecked")
    private MessageConstructor<? extends CorfuMsg> resolveConstructor() {
        // Grab the constructor and get convert it to a lambda.
        try {
            Constructor t = messageType.getRawType().getConstructor();
            MethodHandle mh = lookup.unreflectConstructor(t);
            MethodType mt = MethodType.methodType(Object.class);
            try {
                return (MessageConstructor<? extends CorfuMsg>) LambdaMetafactory.metafactory(lookup,
                        "construct", MethodType.methodType(MessageConstructor.class),
                        mt, mh, mh.type())
                        .getTarget().invokeExact();
            } catch (Throwable th) {
                throw new RuntimeException(th);
            }
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException("CorfuMsgs must include a no-arg constructor!");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}