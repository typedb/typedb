
import store from '@/store';

jest.mock('@/components/shared/PersistentStorage', () => {});

jest.mock('@/components/ServerSettings', () => ({
  getServerHost: () => '127.0.0.1',
  getServerUri: () => '127.0.0.1:48555',
}));

describe('actions', () => {
  store.dispatch('initGrakn');

  test('load keyspaces', async () => {
    expect(store.state.keyspaces).toBeUndefined();
    await store.dispatch('loadKeyspaces');
    expect(store.state.keyspaces).toBeDefined();
  });

  test('create keyspace', async () => {
    expect(store.state.keyspaces).not.toContain('test_keyspace');
    await store.dispatch('createKeyspace', 'test_keyspace');
    await store.dispatch('loadKeyspaces');
    expect(store.state.keyspaces).toContain('test_keyspace');
  });

  test('delete keyspace', async () => {
    await store.dispatch('createKeyspace', 'test_keyspace');
    await store.dispatch('loadKeyspaces');
    expect(store.state.keyspaces).toContain('test_keyspace');
    await store.dispatch('deleteKeyspace', 'test_keyspace');
    await store.dispatch('loadKeyspaces');
    expect(store.state.keyspaces).not.toContain('test_keyspace');
  });
});

