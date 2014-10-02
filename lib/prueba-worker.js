var	DistributedTransactionWorker = require("./distributed-transaction-worker").DistributedTransactionWorker;

var distributedTransactionWorker = DistributedTransactionWorker("worker-nodejs", 'post-ejemplo', 'worker-nodejs-1', false, false, function(data){
    data.prueba = data.prueba + "->Procesamiento en NODJS";

    return data;
});

// LOGGER
var log4js = require('log4js');
var logger = log4js.getLogger('lib/prueba-worker.js');


distributedTransactionWorker.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	distributedTransactionWorker.register();
	distributedTransactionWorker.getTasks();
});