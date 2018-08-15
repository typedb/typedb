import EngineSettings from '@/components/EngineSettings';
import PersistentStorage from '@/components/shared/PersistentStorage';

const DEFAULT_ENGINE_HOST = '127.0.0.1';
const DEFAULT_ENGINE_PORT = '4567';
const DEFAULT_ENGINE_GRPC_PORT = '48555';

jest.mock('@/components/shared/PersistentStorage', () => ({
  get: jest.fn(),
  set: jest.fn(),
}));

// Mock getter behaviour to return anything that has been set by the setter
PersistentStorage.set.mockImplementation((key, value) => {
  PersistentStorage.get.mockImplementation(() => value);
});

describe('EngineHost', () => {
  test('getEngineHostDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const engineHost = EngineSettings.getEngineHost();
    expect(engineHost).toBe(DEFAULT_ENGINE_HOST);
  });

  test('getEngineHostNotDefault', () => {
    EngineSettings.setEngineHost('123.1.1.1');
    const engineHost = EngineSettings.getEngineHost();
    expect(engineHost).toBe('123.1.1.1');
  });
});

describe('EngineGrpcPort', () => {
  test('getEngineGrpcPortDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const engineGrpcPort = EngineSettings.getEngineGrpcPort();
    expect(engineGrpcPort).toBe(DEFAULT_ENGINE_GRPC_PORT);
  });

  test('getEngineGrpcPortNotDefault', () => {
    EngineSettings.setEngineGrpcPort('12345');
    const engineGrpcPort = EngineSettings.getEngineGrpcPort();
    expect(engineGrpcPort).toBe('12345');
  });
});
