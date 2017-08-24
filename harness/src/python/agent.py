from agent import *
import pdb

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


class AgentHandler:
    def createInstance(self, config):
        pass

    def listInstances(self, ):
        return ttypes.InstanceList([])

    def restart(self, instance):
        pass

if __name__ == '__main__':
    handler = AgentHandler()
    processor = Agent.Processor(handler)
    transport = TSocket.TServerSocket(port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print 'Starting server'
    server.serve()