var zookeeper = require('node-zookeeper-client'),
    async = require('async'),
    ZK_ERRORS = require("../conf/zookeeper-errors"),
    CLIENT_ZNODES = require("../conf/zookeeper-znodes").CLIENT_ZNODES,
    TRANSACTION_SUBFIXES = require("../conf/zookeeper-znodes").TRANSACTION_SUBFIXES;

var CreateMode = zookeeper.CreateMode;
function DistributedTransactionClient(clientId, distributedTransactions){
    var that = this;
    this.clientId = clientId;
    this.distributedTransactions = distributedTransactions;
    this.distributedTransactionsDictionary = {};

    this.zk = zookeeper.createClient('localhost:2181');

    this.connect = function(callback){
        console.log("CONECTANDO...");
        that.zk.once('connected', function () {
            console.log('Conectado al servidor.');
            callback(null, "CONNECTED");
        });

        that.zk.connect();
    };

    // Crear recursos necesarios para el funcionamiento del cliente
    this.bootstrap = function(callback){
        var errores = null;
        var resultados = null;

        that.distributedTransactions.forEach(function(distributedTransactionId) {
            console.log(distributedTransactionId);
            async.series({
                transactionClientZnode: function(serieCallback){
                    var path = CLIENT_ZNODES.TRANSACTION_CLIENT_ZNODE(that.clientId, distributedTransactionId);
                    that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                        if(error){
                            switch(error.code){
                            case ZK_ERRORS.NODEEXISTS:
                                console.log("Ya existe al znode de registro de cliente. " + path);
                                serieCallback(null, path);
                                break;
                            default:
                                console.log("Error al crear znode de registro de cliente. " + path);
                                serieCallback(error, null);
                                break;
                            }
                        }else{
                            console.log('Cliente: %s registrado.', createdPath);
                            serieCallback(null, createdPath);
                        }
                    });
                },

                resultsNamespace: function(serieCallback){
                    var path = CLIENT_ZNODES.RESULTS_NAMESPACES(that.clientId, distributedTransactionId);
                    that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                        if(error){
                            switch(error.code){
                            case ZK_ERRORS.NODEEXISTS:
                                console.log("Ya existe al namespace de resultados. " + path);
                                serieCallback(null, path);
                                break;
                            default:
                                console.log("Error al crear namespace de resultados. " + path);
                                serieCallback(error, null);
                                break;
                            }
                        }else{
                            console.log('Namespace: %s creado.', createdPath);
                            serieCallback(null, createdPath);
                        }
                    });
                },

                transactionsNamespace: function(serieCallback){
                    var path = CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, distributedTransactionId);
                    that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                        if(error){
                            switch(error.code){
                            case ZK_ERRORS.NODEEXISTS:
                                console.log("Ya existe al namespace de transacciones. " + path);
                                serieCallback(null, path);
                                break;
                            default:
                                console.log("Error al crear namespace de transacciones. " + path);
                                serieCallback(error, null);
                                break;
                            }
                        }else{
                            console.log('Namespace: %s creado.', createdPath);
                            serieCallback(null, createdPath);
                        }
                    });
                }

            }, function(err, results) {
                if(err && err.length > 0){
                    if(!errores)
                        errores = [];
                    err.forEach(function(error){
                        errores.push(err);
                    });
                }

                if(results && results.length > 0){
                    if(!resultados)
                        resultados = [];
                    results.forEach(function(resultado){
                        resultados.push(resultado);
                    });
                }
            });

        });

        callback(errores, resultados);
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
    // TransaccionId es el id de la transaccion en el sistema de coordinacion
    this.submitTransaction = function(distributedTransactionId, dataObject, callback){
        console.log("CREAR TRANSACCION: " + CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, distributedTransactionId) + "/" + that.clientId + "-" + TRANSACTION_SUBFIXES.TRANSACTION_ZNODE_SUBFIX);
        // Path del znode 
        var path = CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, distributedTransactionId) + "/" + that.clientId + "-" + TRANSACTION_SUBFIXES.TRANSACTION_ZNODE_SUBFIX;
        // Datos
        var data = JSON.stringify( dataObject );
        
        that.zk.create(path, new Buffer(data), CreateMode.PERSISTENT_SEQUENTIAL, function (error, createdPath) {
            if(error){
                switch(error.code){

                case ZK_ERRORS.CONNECTIONLOSS:
                    console.log("ERROR DE CONEXION");
                    that.submitTransaction(distributedTransactionId, dataObject, callback);
                    break;

                default:
                    console.log("Error al crear transaccion. " + path);
                    console.log(error);
                    callback(error, null);
                    break;
                }
            }else{
                var transaccion = path.substring(createdPath.lastIndexOf("/") + 1);
                console.log("Transaccion: " + transaccion);

                // Monitoreamos el estado de la transaccion enviada
                var pathResults = createdPath.replace(CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(clientId, distributedTransactionId), CLIENT_ZNODES.RESULTS_NAMESPACES(clientId, distributedTransactionId));
                console.log("WATCH patResults: " + pathResults);
                watchResults(pathResults, function(){

                });
                    
                callback(null, createdPath);
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
        
        that.zk.exists(path, 
            function (event){
                // path y event.path son lo mismo

                console.log("EH WATCHADO: " + event.path);
                that.zk.getData(event.path, null, that.getDataCallback);
            },
            function (error, stat) {
                if (error) {
                    switch(error.code){
                    case ZK_ERRORS.CONNECTIONLOSS:
                        console.log("ERROR DE CONEXION WATCHRESULT");
                        that.watchResults(path, resultCallback);
                        break;
                    case ZK_ERRORS.NONODE: 
                        break;
                    default:
                        resultCallback(error, null);
                    }
                }

                if (stat) { // Node exists
                    that.zk.getData(path, null, that.getDataCallback);
                } else { // Node does not exists.
                    //console.log("Node does not exists: " + path);
                    //that.watchResults(path, resultCallback);
                }
            }
        );

    };

    this.getDataCallback = function(error, data, stat ){
        if (error) {
            switch(error.code){
            case ZK_ERRORS.CONNECTIONLOSS:
                console.log("ERROR DE CONEXION GETDATA");
                //that.zk.getData(path, null, that.getDataCallback);
                break;
            case ZK_ERRORS.NONODE: 
                break;
            default:
                resultCallback(error, null);
            }
            return;
        }else {
            console.log("TRANSACCION PROCESADA DATA: " + data);
        }
    };

    return this;
}

exports.DistributedTransactionClient = DistributedTransactionClient;