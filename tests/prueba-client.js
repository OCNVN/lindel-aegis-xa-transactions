var DistributedTransactionClient = require("aegis-xa-transactions").XATransactionClient;

var distributedTransactionClient = DistributedTransactionClient("cliente-node6", ['logeo', 'post-ejemplo']);

// LOGGER
var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});
var logger = log4js.getLogger('lib/prueba-client.js');

distributedTransactionClient.init(function(error, results){
	if(error)
		logger.error(error);
	if(results)
		logger.info(results);

	// Enviar una transaccion
	var transactionData = {
		nombres: "miriam del carmen",
		apellidos: "alvarez",
		email: "m.carmen@lindelit.com",
		prueba: "iniciamos miriam"
	};

	distributedTransactionClient.submitTransaction("logeo", transactionData, function(error, result){
		if(!error)
			logger.info("Resultado de la transaccion a logeo: " + result);
		else
			logger.error("Error al ejecutar transaccion: " + error);
	});

	var transactionData2 = {
		nombres: "grman sebastian",
		apellidos: "lucero alvarez",
		email: "german@lindelit.com",
		prueba: "iniciamos german"
	};

	distributedTransactionClient.submitTransaction("post-ejemplo", transactionData2, function(error, result){
		if(!error)
			logger.info("Resultado la transaccion a post-ejemplo: " + result);
		else
			logger.error("Error al ejecutar transaccion: " + error);
	});
});


