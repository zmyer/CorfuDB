package org.corfudb.harness.orchestration;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.corfudb.harness.gen.Agent;
import org.corfudb.harness.gen.FaultType;
import org.corfudb.harness.gen.Instance;
import org.corfudb.harness.gen.InstanceConfig;

/**
 * Created by box on 8/24/17.
 */
public class Harness {
    private final Agent.Client agent;
    private final TTransport transport;

    public Harness(String address, int port) throws Exception {
        transport = new TSocket(address, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        agent = new Agent.Client(protocol);
    }

    public Instance create(String options) throws Exception {
        InstanceConfig config = new InstanceConfig(options);
        config.setConfig(options);
        return agent.create(config);
    }

    public void remove(Instance instance) throws Exception {
        agent.remove(instance);
    }

    public void injectFault(FaultType fault, Instance instance) throws Exception {
        agent.injectFault(fault, instance);
    }

    public void undoFault(FaultType fault, Instance instance) throws Exception {
        agent.undoFault(fault, instance);
    }

    public void close() {
        transport.close();
    }
}
