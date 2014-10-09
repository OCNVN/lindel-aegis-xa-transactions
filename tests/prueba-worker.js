var	XATransactionResource = require("aegis-xa-transactions").XATransactionResource;

var distributedTransactionWorker = XATransactionResource("nodejs-almacenar-mongodb", 'signin-aegis', 'nodejs-almacenar-mongodb-1', false, false, function(data){
    data.prueba = data.prueba + "->NODEJS-almacenar-mongodb";

    return data;
});

// LOGGER
var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});
var logger = log4js.getLogger('lib/prueba-worker.js');


distributedTransactionWorker.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	distributedTransactionWorker.register();
	distributedTransactionWorker.getTasks();
});