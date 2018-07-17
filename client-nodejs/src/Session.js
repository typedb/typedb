const Transaction = require("./Transaction");
const SessionService = require("./service/Session");


/**
 * GraknSession object that can be used to:
 *  - open a new Transaction
 * 
 * @param {String} uri String containing host and port of a valid Grakn server 
 * @param {String} keyspace Grakn keyspace to which this sessions should be bound to
 * @param {Object} credentials Optional object containing user credentials - only used when connecting to a KGMS instance
 */
function GraknSession(uri, keyspace, credentials) {
  this.sessionService = new SessionService(uri, keyspace, credentials);
}

/**
 * Create new Transaction, which is already open and ready to be used.
 * @param {Grakn.txType} txType Type of transaction to open READ, WRITE or BATCH
 * @returns Transaction
 */
GraknSession.prototype.transaction = async function (txType) {
  const transactionService = await this.sessionService.transaction(txType).catch(e => { throw e; });
  return new Transaction(transactionService);
}

/**
 * Close stream connected to gRPC server
 */
GraknSession.prototype.close = function close() {
  this.sessionService.close();
}

module.exports = GraknSession;
