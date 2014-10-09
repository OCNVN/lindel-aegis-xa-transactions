var	XATransactionResource = require("aegis-xa-transactions").XATransactionResource;

var distributedTransactionWorker = XATransactionResource("nodejs-almacenar-elaticsearch", 'signin-aegis', 'nodejs-almacenar-elaticsearch-1', false, true, function(data){
    data.prueba = data.prueba + "->NODJS almacenar elasticsearch";

    return data;
});

// LOGGER
var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});
var logger = log4js.getLogger('lib/prueba-worker3.js');


distributedTransactionWorker.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	distributedTransactionWorker.register();
	distributedTransactionWorker.getTasks();
});