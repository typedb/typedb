import ServerSettings from '@/components/ServerSettings';
import PersistentStorage from '@/components/shared/PersistentStorage';

const DEFAULT_SERVER_HOST = '127.0.0.1';
const DEFAULT_SERVER_PORT = '48555';

jest.mock('@/components/shared/PersistentStorage', () => ({
  get: jest.fn(),
  set: jest.fn(),
}));

// Mock getter behaviour to return anything that has been set by the setter
PersistentStorage.set.mockImplementation((key, value) => {
  PersistentStorage.get.mockImplementation(() => value);
});

describe('ServerHost', () => {
  test('getServerHostDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const serverHost = ServerSettings.getServerHost();
    expect(serverHost).toBe(DEFAULT_SERVER_HOST);
  });

  test('getServerHostNotDefault', () => {
    ServerSettings.setServerHost('123.1.1.1');
    const serverHost = ServerSettings.getServerHost();
    expect(serverHost).toBe('123.1.1.1');
  });
});

describe('ServerPort', () => {
  test('getServerPortDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const serverPort = ServerSettings.getServerPort();
    expect(serverPort).toBe(DEFAULT_SERVER_PORT);
  });

  test('getServerNotDefault', () => {
    ServerSettings.setServerPort('12345');
    const serverPort = ServerSettings.getServerPort();
    expect(serverPort).toBe('12345');
  });
});
