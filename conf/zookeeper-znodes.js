var APPLICATION_ZNODES = {};

// Root
APPLICATION_ZNODES.APP_NAMESPACE = "/XATransactions";
// Namespace de transacciones
APPLICATION_ZNODES.TRANSACTIONS_NAMESPACE = APPLICATION_ZNODES.APP_NAMESPACE + "/transactions";
// Namespace de resultados
APPLICATION_ZNODES.RESULTS_NAMESPACE = APPLICATION_ZNODES.APP_NAMESPACE + "/results";
// Namespace de workers
APPLICATION_ZNODES.WORKERS_NAMESPACE = APPLICATION_ZNODES.APP_NAMESPACE + "/workers";
// Namespace de asignaciones
APPLICATION_ZNODES.ASSIGNS_NAMESPACE = APPLICATION_ZNODES.APP_NAMESPACE + "/assigns";
// Namespace de status
APPLICATION_ZNODES.STATUS_NAMESPACE = APPLICATION_ZNODES.APP_NAMESPACE + "/status";
// Namespace de transacciones-clientes
APPLICATION_ZNODES.TRANSACTION_CLIENT_NAMESPACE = APPLICATION_ZNODES.APP_NAMESPACE + "/transaction-client";


var TRANSACTION_ZNODES = {};

// Namespace de transacciones
TRANSACTION_ZNODES.TRANSACTIONS_NAMESPACE = function(distributedTransactionId){
	return APPLICATION_ZNODES.TRANSACTIONS_NAMESPACE + "/" + distributedTransactionId;
}

// Namespace de resultados
TRANSACTION_ZNODES.RESULTS_NAMESPACE = function(distributedTransactionId){
	return APPLICATION_ZNODES.RESULTS_NAMESPACE + "/" + distributedTransactionId;
}

TRANSACTION_ZNODES.TRANSACTION_CLIENTS_NAMESPACE = function(distributedTransactionId){
	return APPLICATION_ZNODES.TRANSACTION_CLIENT_NAMESPACE + "/" + distributedTransactionId;
}

TRANSACTION_ZNODES.WORKERS_NAMESPACE = function(distributedTransactionId){
	return APPLICATION_ZNODES.WORKERS_NAMESPACE + "/" + distributedTransactionId;
}

TRANSACTION_ZNODES.ASSIGNS_NAMESPACE = function(distributedTransactionId){
	return APPLICATION_ZNODES.ASSIGNS_NAMESPACE + "/" + distributedTransactionId;
}

TRANSACTION_ZNODES.STATUS_NAMESPACE = function(distributedTransactionId){
	return APPLICATION_ZNODES.STATUS_NAMESPACE + "/" + distributedTransactionId;
}





var CLIENT_ZNODES = {};

// Namespace de transacciones
CLIENT_ZNODES.TRANSACTION_CLIENT_ZNODE = function(clientId, distributedTransactionId){
	return TRANSACTION_ZNODES.TRANSACTION_CLIENTS_NAMESPACE(distributedTransactionId) + "/" + clientId;
}

CLIENT_ZNODES.TRANSACTIONS_NAMESPACES = function(clientId, distributedTransactionId){
	return TRANSACTION_ZNODES.TRANSACTIONS_NAMESPACE(distributedTransactionId) + "/" + clientId;
}

// Namespace de resultados
CLIENT_ZNODES.RESULTS_NAMESPACES = function(clientId, distributedTransactionId){
	return TRANSACTION_ZNODES.RESULTS_NAMESPACE(distributedTransactionId) + "/" + clientId;
}




var WORKER_ZNODES = {};

// Namespace de worker
WORKER_ZNODES.WORKER_NAMESPACE = function(workerScheduleName, distributedTransactionId){
	return TRANSACTION_ZNODES.WORKERS_NAMESPACE(distributedTransactionId) + "/" + workerScheduleName;
}

// Namespace de asignaciones
WORKER_ZNODES.ASSIGN_NAMESPACE = function(workerScheduleName, distributedTransactionId){
	return TRANSACTION_ZNODES.ASSIGNS_NAMESPACE(distributedTransactionId) + "/" + workerScheduleName;
}

WORKER_ZNODES.STATUS_NAMESPACE = function(workerScheduleName, distributedTransactionId){
	return TRANSACTION_ZNODES.STATUS_NAMESPACE(distributedTransactionId) + "/" + workerScheduleName;
}






var TRANSACTION_SUBFIXES = {};

// Subfijo para transacciones
TRANSACTION_SUBFIXES.TRANSACTION_ZNODE_SUBFIX = "transaction-";

exports.APPLICATION_ZNODES = APPLICATION_ZNODES;
exports.TRANSACTION_ZNODES = TRANSACTION_ZNODES;
exports.CLIENT_ZNODES = CLIENT_ZNODES;
exports.TRANSACTION_SUBFIXES = TRANSACTION_SUBFIXES;
exports.WORKER_ZNODES = WORKER_ZNODES;