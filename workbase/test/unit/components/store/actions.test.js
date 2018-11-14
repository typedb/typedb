
import store from '@/store';

jest.mock('@/components/shared/PersistentStorage', () => {});
jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));


describe('actions', () => {
// TBD: fix test
//   test('load keyspaces', async () => {
//     const mockGrakn = { keyspaces: () => ({ retrieve: jest.fn() }) };

//     store.commit('setGrakn', mockGrakn);
//     await store.dispatch('loadKeyspaces');

//     expect(mockGrakn.keyspaces().retrieve).toBeCalled();
//   });

  test('create keyspace', async () => {
    const mockGrakn = { session: jest.fn().mockImplementation(() => ({ transaction: () => Promise.resolve() })) };

    store.commit('setGrakn', mockGrakn);


    await store.dispatch('createKeyspace', 'test');

    expect(mockGrakn.session).toBeCalledWith('test');
  });


// TBD: fix test
//   test('delete keyspace', async () => {
//     const mockGrakn = { keyspaces: () => ({ delete: jest.fn().mockImplementation(() => Promise.resolve()) }) };

//     store.commit('setGrakn', mockGrakn);
//     await store.dispatch('deleteKeyspace', 'test');

//     expect(mockGrakn.keyspaces().delete).toBeCalled();
//   });
});

