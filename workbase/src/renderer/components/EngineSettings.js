import storage from './shared/PersistentStorage';

const ENGINE_HOST = 'engine-host';
const ENGINE_GRPC_PORT = 'engine-grpc-port';

const DEFAULT_ENGINE_HOST = '127.0.0.1';
const DEFAULT_ENGINE_GRPC_PORT = '48555';

function getEngineHost() {
  const host = storage.get(ENGINE_HOST);
  if (host) return host;

  storage.set(ENGINE_HOST, DEFAULT_ENGINE_HOST);
  return getEngineHost();
}

function setEngineHost(host) {
  storage.set(ENGINE_HOST, host);
}

function getEngineGrpcPort() {
  const port = storage.get(ENGINE_GRPC_PORT);
  if (port) return port;

  storage.set(ENGINE_GRPC_PORT, DEFAULT_ENGINE_GRPC_PORT);
  return getEngineGrpcPort();
}

function setEngineGrpcPort(port) {
  storage.set(ENGINE_GRPC_PORT, port);
}

function getEngineGrpcUri() {
  return `${this.getEngineHost()}:${this.getEngineGrpcPort()}`;
}

export default {
  getEngineHost,
  setEngineHost,
  getEngineGrpcPort,
  setEngineGrpcPort,
  getEngineGrpcUri,
};
