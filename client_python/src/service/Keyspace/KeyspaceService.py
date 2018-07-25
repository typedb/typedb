import grpc

from src.service.Keyspace.autogenerated.Keyspace_pb2_grpc import KeyspaceServiceStub
import src.service.Keyspace.autogenerated.Keyspace_pb2 as keyspace_messages

class KeyspaceService(object):

    def __init__(self, uri, credentials):

        self.uri = uri
        self.credentials = credentials
        self._channel = grpc.insecure_channel(uri)
        self.stub = KeyspaceServiceStub(self._channel)

    def retrieve(self):
        retrieve_request = keyspace_messages.Keyspace.Retrieve.Req()
        response = self.stub.retrieve(retrieve_request)
        return list(response.names)

    def delete(self, keyspace):
        delete_request = keyspace_messages.Keyspace.Delete.Req()
        delete_request.name = keyspace
        self.stub.delete(delete_request)
        return
