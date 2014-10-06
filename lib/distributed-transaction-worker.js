var zookeeper = require('node-zookeeper-client'),
    async = require('async'),
    ZK_ERRORS = require("../conf/zookeeper-errors"),
    WORKER_ZNODES = require("../conf/zookeeper-znodes").WORKER_ZNODES,
    CLIENT_ZNODES = require("../conf/zookeeper-znodes").CLIENT_ZNODES,
    TRANSACTION_SUBFIXES = require("../conf/zookeeper-znodes").TRANSACTION_SUBFIXES,
    ASSIGN_METADATA_NODES = require("../conf/metadata-descriptions").ASSIGN_METADATA_NODES;

// LOGGER
var log4js = require('log4js');
var logger = log4js.getLogger("lib/distributed-transaction-worker.js");

var CreateMode = zookeeper.CreateMode;
function DistributedTransactionWorker(workerScheduleName, distributedTransactionId, workerId, isFirst, isLast, executable){
	var that = this;
	this.workerId = workerId;
	this.workerScheduleName = workerScheduleName;
	this.distributedTransactionId = distributedTransactionId;
    this.executable = executable;
    this.isLast = isLast;
    this.isFirst = isFirst;

	this.zk = zookeeper.createClient('localhost:2181');

    this.connect = function(callback){
        that.zk.once('connected', function () {
            logger.info('[' + that.workerId + '] Conectado al servidor ');
            callback(null, "CONNECTED");
        });

        that.zk.connect();
    };

    // Crear recursos necesarios para el funcionamiento del cliente
    this.bootstrap = function(callback){

        async.series({
            workerNamespace: function(serieCallback){
                var path = WORKER_ZNODES.WORKER_NAMESPACE(that.workerScheduleName, that.distributedTransactionId);
                that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                    if(error){
                        switch(error.code){
                        case ZK_ERRORS.NODEEXISTS:
                            logger.warn('[' + that.workerId + '] Ya existe al znode de namespace de worker. ' + path);
                            serieCallback(null, path);
                            break;
                        default:
                            logger.error('[' + that.workerId + '] Error al crear znode de namespace de worker. ' + path);
                            serieCallback(error, null);
                            break;
                        }
                    }else{
                        logger.info('[' + that.workerId + '] Namespace: %s creado.', createdPath);
                        serieCallback(null, createdPath);
                    }
                });
            },

            assignNamespace: function(serieCallback){
                var path = WORKER_ZNODES.ASSIGN_NAMESPACE(that.workerScheduleName, that.distributedTransactionId);
                that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                    if(error){
                        switch(error.code){
                    case ZK_ERRORS.NODEEXISTS:
                            logger.warn('[' + that.workerId + '] Ya existe al namespace de asignaciones. ' + path);
                            serieCallback(null, path);
                            break;
                        default:
                            logger.error('[' + that.workerId + '] Error al crear namespace de asignaciones. ' + path);
                            serieCallback(error, null);
                            break;
                        }
                    }else{
                        logger.info('[' + that.workerId + '] Namespace: %s creado.', createdPath);
                        serieCallback(null, createdPath);
                    }
                });
            },

            statusNamespace: function(serieCallback){
                var path = WORKER_ZNODES.STATUS_NAMESPACE(that.workerScheduleName, that.distributedTransactionId);
                that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
                    if(error){
                        switch(error.code){
                    case ZK_ERRORS.NODEEXISTS:
                            logger.warn('[' + that.workerId + '] Ya existe al namespace de status. ' + path);
                            serieCallback(null, path);
                            break;
                        default:
                            logger.error('[' + that.workerId + '] Error al crear namespace de status. ' + path);
                            serieCallback(error, null);
                            break;
                        }
                    }else{
                        logger.info('[' + that.workerId + '] Namespace: %s creado.', createdPath);
                        serieCallback(null, createdPath);
                    }
                });
            }
        }, 

        function(err, results) {
        	that.createAssignNode();
            callback(err, results);
        });
	};

	that.createAssignNode = function(){
		var path = WORKER_ZNODES.ASSIGN_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + that.workerId;
        
        that.zk.create(path, new Buffer(''), CreateMode.PERSISTENT, function (error, createdPath) {
            if(error){
                switch(error.code){
            	case ZK_ERRORS.NODEEXISTS:
                    logger.warn('[' + that.workerId + '] Ya existe al namespace de asignacion. ' + path);
                    break;
                case ZK_ERRORS.CONNECTIONLOSS:
                	that.createAssignNode();
                	break;
                default:
                    logger.error('[' + that.workerId + '] Error al crear namespace de asignaciones. ' + path);
                    break;
                }
            }else{
                logger.info('[' + that.workerId + '] Namespace de asignaciones: %s creado.', createdPath);
            }
        });	
	};

	/* *********************************
	 * *********************************
	 * Registrar el worker en el sistema
	 * *********************************
	 * *********************************
	 */
	that.register = function(){
		var path = WORKER_ZNODES.WORKER_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + that.workerId;
		
		that.zk.create(path, new Buffer('Idle'), CreateMode.EPHEMERAL, function (error, createdPath) {
            if(error){
                switch(error.code){
            	case ZK_ERRORS.NODEEXISTS:
                    logger.warn('[' + that.workerId + '] El worker ya existe: ' + path);
                    break;
                case ZK_ERRORS.CONNECTIONLOSS:
                	that.register();
                	break;
                default:
                    logger.error('[' + that.workerId + '] Error al registrar worker: ' + path);
                    serieCallback(error, null);
                    break;
                }
            }else{
                logger.info('[' + that.workerId + '] Worker registrado exitosamente: ' + createdPath);
            }
        });
	};

	/* ************************************************
	 * ************************************************
	 * Administracion de las tareas asignadas al worker
	 * ************************************************
	 * ************************************************
	 */
	that.getTasks = function(){
		var path = WORKER_ZNODES.ASSIGN_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + that.workerId;
		
		that.zk.getChildren(
			path, 
            // Watcher
			function(event){
				logger.debug('[' + that.workerId + '] Watcher: nuevas tareas');
				that.getTasks();
			},
            that.tasksGetChildrenCallback
		);
	};

    that.tasksGetChildrenCallback = function(error, children, stats){
        var path = WORKER_ZNODES.ASSIGN_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + that.workerId;
        if(error){
            switch(error.code){
            case ZK_ERRORS.CONNECTIONLOSS:
                that.getTasks();
                break;
            default:
                logger.error('[' + that.workerId + '] Error al obtener asignaciones: ' + path);
                break;
            }
        }else{
            logger.debug('[' + that.workerId + '] Nuevas asignaciones obtenidas en path: ' + path);

            if(children){
                children.forEach(function(task){
                    logger.debug('[' + that.workerId + '] Task: ' + task);

                    // Obtener datos de la tarea asignada
                    var dataPath = WORKER_ZNODES.ASSIGN_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + that.workerId + "/" + task;
                    that.getTaskData(dataPath, task);
                });
            }
        }
    };

    that.getTaskData = function(taskPath, task){
        that.zk.getData(
            taskPath,
            function(error, data, stat){
                if(error){
                    switch(error.code){
                    case ZK_ERRORS.CONNECTIONLOSS:
                        that.getTaskData(taskPath);
                        break;
                    default:
                        logger.error('[' + that.workerId + '] Error al obtener datos de tarea: ' + taskPath);
                        break;
                    }
                }else{
                    logger.info('[' + that.workerId + '] Datos de tarea obtenidos exitosamente: ', taskPath);
                    var data = data.toString('utf8')
                    var dataObject = JSON.parse(data);

                    // Procesa la tarea usando los datos recien obtenidos y la funcion executable definida explicitamente
                    var result = that.executable(dataObject);

                    logger.debug('[' + that.workerId + '] Resultado de ejecucion:');
                    console.dir(result);

                    var clientId = result[ASSIGN_METADATA_NODES.XA_ASSIGN_METADATA_NODE][ASSIGN_METADATA_NODES.CLIENT_ID_CHILD];
                    
                    // Si es el ultimo worker en el schedule, debe enviar el resultado directo al cliente
                    if(that.isLast){
                        var resultPath = CLIENT_ZNODES.RESULTS_NAMESPACES(clientId, that.distributedTransactionId) + "/" + task;
                        that.statusCreate(resultPath, JSON.stringify(result));
                    }

                    // Si no es el ultimo worker en el schedule, debe crear un status
                    if(!that.isLast){
                        var statusPath = WORKER_ZNODES.STATUS_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + task;
                        that.statusCreate(statusPath, JSON.stringify(result));
                    }

                    // Eliminar la asignacion de la tarea 
                    var assignPath = WORKER_ZNODES.ASSIGN_NAMESPACE(that.workerScheduleName, that.distributedTransactionId) + "/" + that.workerId + "/" + task;
                    that.assignDelete(assignPath);
                }
            });
    };

    /*
     * Crear status o resultado luego de ser procesado los datos
     */
    that.statusCreate = function (path, data) {
        // La funcion JSON.stringify remplaza las comillas por \", esto causa problemas al parsear 
        // los datos en java, asi que antes de continuar remplazamos \" por '
        data.replace('\"', "'");

        that.zk.create(path,
            new Buffer(data),
            CreateMode.PERSISTENT,
            function(error, path){
                if (error) {
                    switch(error.code){
                    case ZK_ERRORS.CONNECTIONLOSS:
                        logger.warn('[' + that.workerId + '] Conexion perdida al crear Status/Result de tarea: ' + path);
                        that.statusCreate(path, data);
                        break;
                    case ZK_ERRORS.NODEEXISTS:
                        logger.warn('[' + that.workerId + '] Status/Result de tarea ya existente: ' + path);
                        break;
                    default:
                        logger.error('[' + that.workerId + '] Error al crear Status/Result de tarea: ' + path);
                        break;
                    }
                }else{
                    logger.info('[' + that.workerId + '] Status/Result de tarea creado exitosamente: ' + path);
                }
            });
    };

    /*
     * Eliminar asignacion ya procesada
     */
    that.assignDelete = function (path){
        that.zk.remove(path, -1 , function(error){
            if (error) {
                switch(error.code){
                case ZK_ERRORS.CONNECTIONLOSS:
                    that.assignDelete(path);
                    break;
                default:
                    logger.error('[' + that.workerId + '] Error al elminar la asignacion de tarea:: ' + path);
                    console.dir(error);
                    break;
                }
            }else{
                logger.info('[' + that.workerId + '] Asignacion de tarea eliminada exitosamente: ' + path);
            }
        });
    }

    /*
     * Conexion y bootstraping
     */
    this.init = function(callback){
        logger.debug('[' + that.workerId + '] INICIANDO TRANSACTION WORKER');
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

	return this;
};

exports.DistributedTransactionWorker = DistributedTransactionWorker;