import Vue from 'vue';
import Vuex from 'vuex';

import { UPDATE_METATYPE_INSTANCES } from '@/components/shared/StoresActions';

import getters from '@/components/SchemaDesign/store/getters';
import mutations from '@/components/SchemaDesign/store/mutations';
import actions from '@/components/SchemaDesign/store/actions';
import state from '@/components/SchemaDesign/store/state';
import store from '@/store';
import MockConcepts from '../../../../helpers/MockConcepts';


jest.mock('@/../Logger', () => ({ error: () => {} }));

jest.mock('@/components/shared/PersistentStorage', () => ({
}));

jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));


jest.mock('@/components/ServerSettings', () => ({
  getServerHost: () => '127.0.0.1',
  getServerUri: () => '127.0.0.1:48555',
}));

Vue.use(Vuex);

jest.setTimeout(30000);

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };


describe('actions', () => {
  test('UPDATE_METATYPE_INSTANCES', async () => {
    const graknSession = {
      transaction: () => Promise.resolve({
        query: () => Promise.resolve({ collectConcepts: () => Promise.resolve([MockConcepts.getMockEntityType()]) }),
        close: jest.fn(),
      }),
    };

    store.registerModule('schema-design', { namespaced: true, getters, state, mutations, actions });

    store.commit('schema-design/graknSession', graknSession);

    await store.dispatch(`schema-design/${UPDATE_METATYPE_INSTANCES}`).then(() => {
    });
  });
});

