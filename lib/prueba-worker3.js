var	DistributedTransactionWorker = require("./distributed-transaction-worker").DistributedTransactionWorker;

var distributedTransactionWorker = DistributedTransactionWorker("nodejs-almacenar-elaticsearch", 'signin-aegis', 'nodejs-almacenar-elaticsearch-1', false, true, function(data){
    data.prueba = data.prueba + "->NODJS almacenar elasticsearch";

    return data;
});

// LOGGER
var log4js = require('log4js');
var logger = log4js.getLogger('lib/prueba-worker3.js');


distributedTransactionWorker.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	distributedTransactionWorker.register();
	distributedTransactionWorker.getTasks();
});