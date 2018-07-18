// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('grpc');
var Keyspace_pb = require('./Keyspace_pb.js');

function serialize_keyspace_Keyspace_Create_Req(arg) {
  if (!(arg instanceof Keyspace_pb.Keyspace.Create.Req)) {
    throw new Error('Expected argument of type keyspace.Keyspace.Create.Req');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_keyspace_Keyspace_Create_Req(buffer_arg) {
  return Keyspace_pb.Keyspace.Create.Req.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_keyspace_Keyspace_Create_Res(arg) {
  if (!(arg instanceof Keyspace_pb.Keyspace.Create.Res)) {
    throw new Error('Expected argument of type keyspace.Keyspace.Create.Res');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_keyspace_Keyspace_Create_Res(buffer_arg) {
  return Keyspace_pb.Keyspace.Create.Res.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_keyspace_Keyspace_Delete_Req(arg) {
  if (!(arg instanceof Keyspace_pb.Keyspace.Delete.Req)) {
    throw new Error('Expected argument of type keyspace.Keyspace.Delete.Req');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_keyspace_Keyspace_Delete_Req(buffer_arg) {
  return Keyspace_pb.Keyspace.Delete.Req.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_keyspace_Keyspace_Delete_Res(arg) {
  if (!(arg instanceof Keyspace_pb.Keyspace.Delete.Res)) {
    throw new Error('Expected argument of type keyspace.Keyspace.Delete.Res');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_keyspace_Keyspace_Delete_Res(buffer_arg) {
  return Keyspace_pb.Keyspace.Delete.Res.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_keyspace_Keyspace_Retrieve_Req(arg) {
  if (!(arg instanceof Keyspace_pb.Keyspace.Retrieve.Req)) {
    throw new Error('Expected argument of type keyspace.Keyspace.Retrieve.Req');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_keyspace_Keyspace_Retrieve_Req(buffer_arg) {
  return Keyspace_pb.Keyspace.Retrieve.Req.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_keyspace_Keyspace_Retrieve_Res(arg) {
  if (!(arg instanceof Keyspace_pb.Keyspace.Retrieve.Res)) {
    throw new Error('Expected argument of type keyspace.Keyspace.Retrieve.Res');
  }
  return new Buffer(arg.serializeBinary());
}

function deserialize_keyspace_Keyspace_Retrieve_Res(buffer_arg) {
  return Keyspace_pb.Keyspace.Retrieve.Res.deserializeBinary(new Uint8Array(buffer_arg));
}


var KeyspaceServiceService = exports.KeyspaceServiceService = {
  create: {
    path: '/keyspace.KeyspaceService/create',
    requestStream: false,
    responseStream: false,
    requestType: Keyspace_pb.Keyspace.Create.Req,
    responseType: Keyspace_pb.Keyspace.Create.Res,
    requestSerialize: serialize_keyspace_Keyspace_Create_Req,
    requestDeserialize: deserialize_keyspace_Keyspace_Create_Req,
    responseSerialize: serialize_keyspace_Keyspace_Create_Res,
    responseDeserialize: deserialize_keyspace_Keyspace_Create_Res,
  },
  retrieve: {
    path: '/keyspace.KeyspaceService/retrieve',
    requestStream: false,
    responseStream: false,
    requestType: Keyspace_pb.Keyspace.Retrieve.Req,
    responseType: Keyspace_pb.Keyspace.Retrieve.Res,
    requestSerialize: serialize_keyspace_Keyspace_Retrieve_Req,
    requestDeserialize: deserialize_keyspace_Keyspace_Retrieve_Req,
    responseSerialize: serialize_keyspace_Keyspace_Retrieve_Res,
    responseDeserialize: deserialize_keyspace_Keyspace_Retrieve_Res,
  },
  delete: {
    path: '/keyspace.KeyspaceService/delete',
    requestStream: false,
    responseStream: false,
    requestType: Keyspace_pb.Keyspace.Delete.Req,
    responseType: Keyspace_pb.Keyspace.Delete.Res,
    requestSerialize: serialize_keyspace_Keyspace_Delete_Req,
    requestDeserialize: deserialize_keyspace_Keyspace_Delete_Req,
    responseSerialize: serialize_keyspace_Keyspace_Delete_Res,
    responseDeserialize: deserialize_keyspace_Keyspace_Delete_Res,
  },
};

exports.KeyspaceServiceClient = grpc.makeGenericClientConstructor(KeyspaceServiceService);
