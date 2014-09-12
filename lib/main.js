var DistributedTransactionClient = require("./distributed-transaction-client").DistributedTransactionClient;

var distributedTransactionClient = DistributedTransactionClient("cliente-node6", ['logeo', 'post-ejemplo']);

distributedTransactionClient.init(function(error, results){

	// Enviar una transaccion
	var transactionData = {
		nombres: "miriam del carmen",
		apellidos: "alvarez",
		email: "m.carmen@lindelit.com",
		prueba: "iniciamos miriam"
	};

	distributedTransactionClient.submitTransaction("logeo", transactionData, function(error, result){
		if(!error)
			console.log("Creamos la transaccion a logeo: " + result);
	});

	var transactionData2 = {
		nombres: "grman sebastian",
		apellidos: "lucero alvarez",
		email: "german@lindelit.com",
		prueba: "iniciamos german"
	};

	distributedTransactionClient.submitTransaction("post-ejemplo", transactionData2, function(error, result){
		if(!error)
			console.log("Creamos la transaccion a post-ejemplo: " + result);
	});
});


