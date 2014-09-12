var zookeeper = require("zookeeper");
var DistributedTransactionClient = require("./distributed-transaction-client").DistributedTransactionClient;

var distributedTransactionClient = DistributedTransactionClient("cliente-node2", []);

/*var CLIENT_ZNODES = require("../conf/zookeeper-znodes").CLIENT_ZNODES;

console.log("RESULTS_NAMESPACE: " + CLIENT_ZNODES.RESULTS_NAMESPACE("cliente-1", "logeo"));
console.log("TRANSACTIONS_NAMESPACE: " + CLIENT_ZNODES.TRANSACTIONS_NAMESPACE("cliente-1", "logeo"));

var zk = new zookeeper({
	connect: "localhost:2181"
	,timeout: 200000
	,debug_level: zookeeper.ZOO_LOG_LEVEL_ERROR
	,host_order_deterministic: false
});

zk.connect(function (err) {
    if(err) throw err;
    
    console.log ("zk session established, id=%s", zk.client_id);
    

    zk.a_create ("/node.js1", "some value", zookeeper.ZOO_SEQUENCE | zookeeper.ZOO_EPHEMERAL, function (rc, error, path)  {
        if (rc != 0) {
            console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
        } else {
            console.log ("created zk node %s", path);
            process.nextTick(function () {
                zk.close ();
            });
        }
    });
});*/