// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('grpc');
var Session_pb = require('./Session_pb.js');
var Concept_pb = require('./Concept_pb.js');
var Answer_pb = require('./Answer_pb.js');

function serialize_session_Transaction_Req(arg) {
  if (!(arg instanceof Session_pb.Transaction.Req)) {
    throw new Error('Expected argument of type session.Transaction.Req');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_session_Transaction_Req(buffer_arg) {
  return Session_pb.Transaction.Req.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_session_Transaction_Res(arg) {
  if (!(arg instanceof Session_pb.Transaction.Res)) {
    throw new Error('Expected argument of type session.Transaction.Res');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_session_Transaction_Res(buffer_arg) {
  return Session_pb.Transaction.Res.deserializeBinary(new Uint8Array(buffer_arg));
}


var SessionServiceService = exports.SessionServiceService = {
  // Represents a full transaction. The stream of `Transaction.Req`s must begin with a `Open` message.
  // When the call is completed, the transaction will always be closed, with or without a `Commit` message.
  transaction: {
    path: '/session.SessionService/transaction',
    requestStream: true,
    responseStream: true,
    requestType: Session_pb.Transaction.Req,
    responseType: Session_pb.Transaction.Res,
    requestSerialize: serialize_session_Transaction_Req,
    requestDeserialize: deserialize_session_Transaction_Req,
    responseSerialize: serialize_session_Transaction_Res,
    responseDeserialize: deserialize_session_Transaction_Res,
  },
};

exports.SessionServiceClient = grpc.makeGenericClientConstructor(SessionServiceService);
