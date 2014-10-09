var XATransactionResource = require('./xa-transaction-resource');
var XATransactionClient = require('./xa-transaction-client');

function AegisXATransactions(){

}

AegisXATransactions.prototype.XATransactionResource = XATransactionResource;

AegisXATransactions.prototype.XATransactionClient = XATransactionClient;

module.exports = new AegisXATransactions;