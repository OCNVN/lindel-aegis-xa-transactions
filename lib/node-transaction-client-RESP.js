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
                that.zk.a_create (CLIENT_ZNODES.RESULTS_NAMESPACES(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
                    if(ZK_ERRORS.OK == rc || ZK_ERRORS.NODEEXISTS == rc)
                        serieCallback(null, path);
                    else
                        serieCallback(error, null);
                });
            },

            transactionsNamespace: function(serieCallback){
                that.zk.a_create (CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, "logeo"), "", zookeeper.ZOO_PERSISTENT, function (rc, error, path)  {
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
        
        console.log("CREAR ZNODE: " + CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, distributedTransactionId));
        
        that.zk.a_create (
            CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, distributedTransactionId) + "/" + that.clientId + "-" + TRANSACTION_SUBFIXES.TRANSACTION_ZNODE_SUBFIX, 
            JSON.stringify( dataObject ), 
            zookeeper.ZOO_SEQUENCE | zookeeper.ZOO_PERSISTENT, 
            function (rc, error, path)  {
                switch (rc){
                case ZK_ERRORS.OK: 
                    var transaccion = path.substring(path.lastIndexOf("/") + 1);
                    console.log("Transaccion: " + transaccion);

                    // Monitoreamos el estado de la transaccion enviada
                    watchResults(path.replace(CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(clientId, distributedTransactionId), CLIENT_ZNODES.RESULTS_NAMESPACES(clientId, distributedTransactionId)), function(){

                    });
                    
                    callback(null, path);
                    break;

                case ZK_ERRORS.CONNECTIONLOSS:
                    console.log("ERROR DE CONEXION");
                    that.submitTransaction(distributedTransactionId, dataObject, callback);
                    break;

                default: 
                    callback(rc, null);
                    break;
                }
        });
    };

    /*
     * Monitorear el resultado de la transaccion enviada
     */
    this.watchResults = function(path, resultCallback){
        console.log("EXISTS?: " + path);
        // Colocar watcher para ser notificados de la existencia del resultado
        // y el segundo callback hace la lectura por si ya existe
        that.zk.a_exists(path, 
            function(type, state, pathWatcher){
                // Cuando el znode sea creado obtenemos los datos del znode
                // q contienen informacion de la transaccion ejecutada
                console.log("EH WATCHADO: " + pathWatcher);
                that.zk.a_get(pathWatcher, null, that.getDataCallback);
            }, function(rc, error, stat){
                console.log("RESULT CALLBACK ERROR: " + error);
                switch (rc){
                case ZK_ERRORS.OK: 
                    if(stat){
                        that.zk.a_get(path, null, that.getDataCallback);
                    }
                    break;

                case ZK_ERRORS.CONNECTIONLOSS:
                    console.log("ERROR DE CONEXION");
                    that.watchResults(path, resultCallback);
                    break;
                case ZK_ERRORS.NONODE: 
                    break;
                default: 
                    resultCallback(rc, null);
                    break;
                }
            }
        );
    };

    this.getDataCallback = function(rc, error, stat, data ){
        switch (rc){
        case ZK_ERRORS.OK: 
            console.log("TRANSACCION PROCESADA DATA: " + data);
            break;

        case ZK_ERRORS.CONNECTIONLOSS:
            console.log("ERROR DE CONEXION");
            that.zk.a_get(path, null, that.getDataCallback);
            break;

        default: 
            resultCallback(rc, null);
            break;
        }
    };

    return this;
}

exports.DistributedTransactionClient = DistributedTransactionClient;