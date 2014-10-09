var	XATransactionResource = require("aegis-xa-transactions").XATransactionResource;

var distributedTransactionWorker = XATransactionResource("nodejs-almacenar-redis", 'signin-aegis', 'nodejs-almacenar-redis-1', false, false, function(data){
    data.prueba = data.prueba + "->NODJS almacenar redis";

    return data;
});

// LOGGER
var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});
var logger = log4js.getLogger('lib/prueba-worker2.js');


distributedTransactionWorker.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	distributedTransactionWorker.register();
	distributedTransactionWorker.getTasks();
});