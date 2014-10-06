var	DistributedTransactionWorker = require("./distributed-transaction-worker").DistributedTransactionWorker;

var distributedTransactionWorker = DistributedTransactionWorker("nodejs-almacenar-redis", 'signin-aegis', 'nodejs-almacenar-redis-1', false, false, function(data){
    data.prueba = data.prueba + "->NODJS almacenar redis";

    return data;
});

// LOGGER
var log4js = require('log4js');
var logger = log4js.getLogger('lib/prueba-worker2.js');


distributedTransactionWorker.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	distributedTransactionWorker.register();
	distributedTransactionWorker.getTasks();
});