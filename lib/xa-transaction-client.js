var zookeeper = require('node-zookeeper-client'),
    async = require('async'),
    ZK_ERRORS = require("../conf/zookeeper-errors"),
    CLIENT_ZNODES = require("../conf/zookeeper-znodes").CLIENT_ZNODES,
    TRANSACTION_SUBFIXES = require("../conf/zookeeper-znodes").TRANSACTION_SUBFIXES;

// LOGGER
var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});
var logger = log4js.getLogger("lib/xa-transaction-client.js");

var CreateMode = zookeeper.CreateMode;
function XATransactionClient(clientId, distributedTransactions){
    var that = this;
    this.clientId = clientId;
    this.distributedTransactions = distributedTransactions;
    this.distributedTransactionsDictionary = {};

    this.zk = zookeeper.createClient('localhost:2181');

    this.connect = function(callback){
        that.zk.once('connected', function () {
            logger.info('Conectado al servidor ');
            callback(null, "CONNECTED");
        });

        that.zk.connect();
    };

    // Crear recursos necesarios para el funcionamiento del cliente
    this.bootstrap = function(callback){
        var errores = null;
        var resultados = null;

        var distributedTransactionsCount = that.distributedTransactions.length;

        logger.debug("Distributed transactions: " + that.distributedTransactions);

        that.distributedTransactions.forEach(function(distributedTransactionId, index) {

            async.series({
                transactionClientZnode: function(serieCallback){
                    var path = CLIENT_ZNODES.TRANSACTION_CLIENT_ZNODE(that.clientId, distributedTransactionId);
                    that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                        if(error){
                            switch(error.code){
                            case ZK_ERRORS.NODEEXISTS:
                                logger.warn("Ya existe al znode de registro de cliente. " + path);
                                serieCallback(null, path);
                                break;
                            default:
                                logger.error("Error al crear znode de registro de cliente. " + path);
                                serieCallback(error, null);
                                break;
                            }
                        }else{
                            logger.info('Cliente: %s registrado.', createdPath);
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
                                logger.warn("Ya existe al namespace de resultados. " + path);
                                serieCallback(null, path);
                                break;
                            default:
                                logger.error("Error al crear namespace de resultados. " + path);
                                serieCallback(error, null);
                                break;
                            }
                        }else{
                            logger.info('Namespace: %s creado.', createdPath);
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
                                logger.warn("Ya existe al namespace de transacciones. " + path);
                                serieCallback(null, path);
                                break;
                            default:
                                logger.error("Error al crear namespace de transacciones. " + path);
                                serieCallback(error, null);
                                break;
                            }
                        }else{
                            logger.info('Namespace de tranacciones: %s creado.', createdPath);
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

                // Revisar esto podria estar mal, nada asegura que el async de
                // cada transaccion distribuida se ejecute en serie, dependiendo del
                // tiempo de latencia de cada async, el tiempo en que termine de ejecutarse
                // y por lo tanto el orden de termino de ejecucion puede variar
                if(distributedTransactionsCount -1 == index)
                    callback(errores, resultados);

            });

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
    // TransaccionId es el id de la transaccion en el sistema de coordinacion
    this.submitTransaction = function(distributedTransactionId, dataObject, callback){
        // Path del znode 
        var path = CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(that.clientId, distributedTransactionId) + "/" + that.clientId + "-" + TRANSACTION_SUBFIXES.TRANSACTION_ZNODE_SUBFIX;
        // Datos
        var data = JSON.stringify( dataObject );
        
        that.zk.create(path, new Buffer(data), CreateMode.PERSISTENT_SEQUENTIAL, function (error, createdPath) {
            if(error){
                switch(error.code){

                case ZK_ERRORS.CONNECTIONLOSS:
                    logger.warn("ERROR DE CONEXION SUBMITTRANSACCION: " + path);
                    that.submitTransaction(distributedTransactionId, dataObject, callback);
                    break;

                default:
                    logger.error("Error al crear transaccion. " + path);
                    logger.error(error);
                    callback(error, null);
                    break;
                }
            }else{
                logger.info("Transaccion creada: " + createdPath);

                // Monitoreamos el estado de la transaccion enviada
                var pathResults = createdPath.replace(CLIENT_ZNODES.TRANSACTIONS_NAMESPACES(clientId, distributedTransactionId), CLIENT_ZNODES.RESULTS_NAMESPACES(clientId, distributedTransactionId));

                that.watchResults(pathResults, callback);
            }
        });

    };

    /*
     * Monitorear el resultado de la transaccion enviada
     */
    this.watchResults = function(path, resultCallback){
        // Colocar watcher para ser notificados de la existencia del resultado
        // y el segundo callback hace la lectura por si ya existe
        
        that.zk.exists(path, 
            function (event){
                // path y event.path son lo mismo

                that.getResultData(event.path, resultCallback);
            },
            function (error, stat) {
                if (error) {
                    switch(error.code){
                    case ZK_ERRORS.CONNECTIONLOSS:
                        logger.warn("ERROR DE CONEXION WATCHRESULT: " + path);
                        that.watchResults(path, resultCallback);
                        break;
                    case ZK_ERRORS.NONODE: 
                        break;
                    default:
                        resultCallback(error, null);
                        break;
                    }
                }

                if (stat) { // Node exists
                    that.getResultData(path, resultCallback);
                } else { // Node does not exists.
                }
            }
        );

    };

    this.getResultData = function(path, resultCallback){
        that.zk.getData(path, null, function(error, data, stat){
            if (error) {
                switch(error.code){
                case ZK_ERRORS.CONNECTIONLOSS:
                    logger.log("ERROR DE CONEXION getReultData: " + path);
                    that.getResultData(path, resultCallback);
                    break;
                case ZK_ERRORS.NONODE: 
                    break;
                default:
                    resultCallback(error, null);
                    break;
                }
            }else {
                logger.info('Transaccion procesada exitosamente, retornando resultado: ' + path);
                resultCallback(null, data);
                // Borrar el resultado de la transaccion
                that.zk.remove(path, -1, that.transactionResultDeleteCallback);
            }
        });
    };

    this.transactionResultDeleteCallback = function(error){
        if(error){
            switch(error.code){
            case ZK_ERRORS.CONNECTIONLOSS:
                logger.log("ERROR DE CONEXION AL ELIMIONAR RESULT: " + path);
                that.zk.remove(error.path, -1, that.transactionResultDeleteCallback);
                break;
            case ZK_ERRORS.NONODE: 
                break;
            default:
                logger.error("Error al eliminar resultado: " + path); 
                logger.error(error.stack);
                resultCallback(error, null);
                break;
            }
        }
        else{
            logger.debug('Resultado eliminado. ');
        }
       
    };

    return this;
};

module.exports = XATransactionClient;