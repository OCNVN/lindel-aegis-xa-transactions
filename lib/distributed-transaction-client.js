var zookeeper = require("zookeeper"),
    async = require('async'),
    ZK_ERRORS = require("../conf/zookeeper-errors"),
    CLIENT_ZNODES = require("../conf/zookeeper-znodes").CLIENT_ZNODES,
    TRANSACTION_SUBFIXES = require("../conf/zookeeper-znodes").TRANSACTION_SUBFIXES;

function DistributedTransactionClient(clientId, distributedTransactions){
    var that = this;
    this.clientId = clientId;
    this.distributedTransactions = distributedTransactions;
    this.distributedTransactionsDictionary = {};

    this.zk = new zookeeper({
        connect: "localhost:2181"
        ,timeout: 200000
        ,debug_level: zookeeper.ZOO_LOG_LEVEL_ERROR
        ,host_order_deterministic: false
    });

    this.connect = function(callback){
        that.zk.connect(function (err, result) {
            if(!err){
                console.log ("zk session established, id=%s", that.zk.client_id);
            }
            callback(err, result);
        });
    };

    // Crear recursos necesarios para el funcionamiento del cliente
    this.bootstrap = function(callback){
        
        async.series({
            transactionClientZnode: function(serieCallback){
                that.zk.a_create (CLIENT_ZNODES.TRANSACTION_CLIENT_ZNODE(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
                    if(ZK_ERRORS.OK == rc || ZK_ERRORS.NODEEXISTS == rc)
                        serieCallback(null, path);
                    else
                        serieCallback(error, null);
                });
            },

            resultsNamespace: function(serieCallback){
                that.zk.a_create (CLIENT_ZNODES.RESULTS_NAMESPACE(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
                    if(ZK_ERRORS.OK == rc || ZK_ERRORS.NODEEXISTS == rc)
                        serieCallback(null, path);
                    else
                        serieCallback(error, null);
                });
            },

            transactionsNamespace: function(serieCallback){
                that.zk.a_create (CLIENT_ZNODES.TRANSACTIONS_NAMESPACE(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
                    if(ZK_ERRORS.OK == rc || ZK_ERRORS.NODEEXISTS == rc)
                        serieCallback(null, path);
                    else
                        serieCallback(error, null);
                });
            }

        }, function(err, results) {

            callback(err, results);
        });

    };

    /*
     * Conexion y bootstraping
     */
    this.init = function(callback){
        async.series({
            connect: function(serieCallback){
                that.connect(serieCallback);
            },

            bootstrap: function(serieCallback){
                that.bootstrap(serieCallback);
            }
        }, function(error, results){
            callback(error, results);
        })

    };

    /* **********************
     * **********************
     * ENVIO DE TRANSACCIONES 
     * **********************
     * **********************
     **/
    // TransaccionId es el nombre de la transaccion en el sistema de coordinacion
    this.submitTransaction = function(distributedTransactionId, dataObject, callback){
        console.log("CREAR ZNODE: " + CLIENT_ZNODES.TRANSACTIONS_NAMESPACE(that.clientId, distributedTransactionId));
        that.zk.a_create (
            CLIENT_ZNODES.TRANSACTIONS_NAMESPACE(that.clientId, distributedTransactionId) + "/" + that.clientId + "-" + TRANSACTION_SUBFIXES.TRANSACTION_ZNODE_SUBFIX, 
            JSON.stringify( dataObject ), 
            zookeeper.ZOO_SEQUENCE | zookeeper.ZOO_PERSISTENT, 
            function (rc, error, path)  {
                if(ZK_ERRORS.OK == rc)
                    callback(null, path);
                else
                    callback(rc, null);
                
        });
    };

    return this;
}

exports.DistributedTransactionClient = DistributedTransactionClient;