import storage from './shared/PersistentStorage';

const SERVER_HOST = 'server-host';
const SERVER_PORT = 'server-port';

const DEFAULT_SERVER_HOST = '127.0.0.1';
const DEFAULT_SERVER_PORT = '48555';

function getServerHost() {
  const host = storage.get(SERVER_HOST);
  if (host) return host;

  storage.set(SERVER_HOST, DEFAULT_SERVER_HOST);
  return getServerHost();
}

function setServerHost(host) {
  storage.set(SERVER_HOST, host);
}

function getServerPort() {
  const port = storage.get(SERVER_PORT);
  if (port) return port;

  storage.set(SERVER_PORT, DEFAULT_SERVER_PORT);
  return getServerPort();
}

function setServerPort(port) {
  storage.set(SERVER_PORT, port);
}

function getServerUri() {
  return `${this.getServerHost()}:${this.getServerPort()}`;
}

export default {
  getServerHost,
  setServerHost,
  getServerPort,
  setServerPort,
  getServerUri,
};
