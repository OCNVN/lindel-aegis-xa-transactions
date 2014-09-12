var zookeeper = require("zookeeper");
var CLIENT_ZNODES = require("../conf/zookeeper-znodes").CLIENT_ZNODES;

function DistributedTransactionClient(clientId, distributedTransactions){
    this.clientId = clientId;
    this.distributedTransactions = distributedTransactions;
    this.distributedTransactionsDictionary = {};

    this.zk = new zookeeper({
        connect: "localhost:2181"
        ,timeout: 200000
        ,debug_level: zookeeper.ZOO_LOG_LEVEL_ERROR
        ,host_order_deterministic: false
    });

    var that = this;
    
    this.zk.connect(function (err) {
        if(err) throw err;
        console.log ("zk session established, id=%s", that.zk.client_id);

        // Bootstrap
        that.bootstrap();
        console.log("bootstraping client");

    });

    // Crear recursos necesarios para el funcionamiento del cliente
    this.bootstrap = function(){
        console.log("HACIENDO BOOTSTRAP");

        that.zk.a_create (CLIENT_ZNODES.TRANSACTION_CLIENT_ZNODE(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
            if (rc != 0) {
                console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
            } else {
                console.log ("created zk node %s", path);
                process.nextTick(function () {
                    //that.zk.close ();
                });
            }
        });

        that.zk.a_create (CLIENT_ZNODES.RESULTS_NAMESPACE(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
            if (rc != 0) {
                console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
            } else {
                console.log ("created zk node %s", path);
                process.nextTick(function () {
                    //that.zk.close ();
                });
            }
        });

        that.zk.a_create (CLIENT_ZNODES.TRANSACTIONS_NAMESPACE(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
            if (rc != 0) {
                console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
            } else {
                console.log ("created zk node %s", path);
                process.nextTick(function () {
                    //that.zk.close ();
                });
            }
        });
    }
}

exports.DistributedTransactionClient = DistributedTransactionClient;