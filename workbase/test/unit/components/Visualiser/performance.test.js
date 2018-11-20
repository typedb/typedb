import Vue from 'vue';
import Vuex from 'vuex';

import { RUN_CURRENT_QUERY, CURRENT_KEYSPACE_CHANGED } from '@/components/shared/StoresActions';

import getters from '@/components/Visualiser/store/getters';
import mutations from '@/components/Visualiser/store/mutations';
import actions from '@/components/Visualiser/store/actions';

import store from '@/store';

import TabState from '@/components/Visualiser/store/tabState';

jest.mock('@/../Logger', () => ({ error: () => {} }));

jest.mock('@/components/shared/PersistentStorage', () => ({
}));

jest.mock('@/components/ServerSettings', () => ({
  getServerHost: () => '127.0.0.1',
  getServerUri: () => '127.0.0.1:48555',
}));

jest.mock('@/components/Visualiser/RightBar/SettingsTab/QuerySettings', () => ({
  getRolePlayersStatus: () => true,
  getNeighboursLimit: () => 1,
}));

jest.mock('@/components/Visualiser/RightBar/SettingsTab/DisplaySettings', () => ({
  getTypeLabels: () => [],
}));

Vue.use(Vuex);

jest.setTimeout(30000);

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

beforeAll(async () => {
  store.registerModule('tab-0', { namespaced: true, getters, state: TabState.create(), mutations, actions });

  store.dispatch('initGrakn');

  const visFacade = {
    updateNode: jest.fn(),
    fitGraphToWindow: jest.fn(),
    addToCanvas: jest.fn(),
    resetCanvas: jest.fn(),
    getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
  };

  store.commit('tab-0/setVisFacade', visFacade);

  store.dispatch(`tab-0/${CURRENT_KEYSPACE_CHANGED}`, 'gene');
});

describe('Run Query', () => {
  test('match $x isa person', async () => {
    store.commit('tab-0/setCurrentQuery', 'match $x isa person; limit 1; get;');

    const t0 = performance.now();
    await store.dispatch(`tab-0/${RUN_CURRENT_QUERY}`);
    const t1 = performance.now();

    expect((t1 - t0) < 200).toBe(true);
  });
});

