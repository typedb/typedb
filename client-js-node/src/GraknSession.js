const grpc = require("grpc");
const GraknTx = require("./GraknTx");
const RemoteSession = require("./service/Session");


/**
 * Creates new GraknSession object that can be used to:
 *  - open a new GraknTx
 *  - delete Keyspace
 * 
 * @param {String} uri String containing host and port of a valid Grakn server 
 * @param {String} keyspace Grakn keyspace to which this sessions should be bound to
 * @param {Object} credentials Optional object containing user credentials - only used when connecting to a KGMS instance
 */
function GraknSession(uri, keyspace, credentials) {
  this.remoteSession = new RemoteSession(uri, keyspace, credentials);
}

/**
 * Method used to create new GraknTx, which also send Open request to make the tx ready to be used.
 * @param {GraknSession.txType} txType Type of transaction to open READ, WRITE or BATCH
 * @returns GraknTx
 */
GraknSession.prototype.transaction = async function (txType) {
  const remoteTx = await this.remoteSession.createRemoteTx(txType);
  return new GraknTx(remoteTx);
}

/**
 * Method used to close current Grakn client
 */
GraknSession.prototype.close = function close() {
  this.remoteSession.close();
}

module.exports = GraknSession;
