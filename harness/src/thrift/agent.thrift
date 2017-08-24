namespace java org.corfudb.harness.gen

struct InstanceConfig {
  1: required string config
}

struct Instance {
  1: required string id
  2: required string address
  3: required i32 port
  4: required InstanceConfig config
}

struct InstanceList {
  1: list<Instance> instances;
}

enum FaultType {
  Jitter
}

service Agent {
  Instance create(1: InstanceConfig config)
  InstanceList listInstances()
  void restart(1: Instance instance)
  void remove(1: Instance instance)
  void injectFault(1: FaultType fault, 2: Instance instance)
  void undoFault(1: FaultType fault, 2: Instance instance)
}