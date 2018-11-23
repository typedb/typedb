import Vue from 'vue';
import Vuex from 'vuex';

import {
  CURRENT_KEYSPACE_CHANGED,
  OPEN_GRAKN_TX,
  UPDATE_METATYPE_INSTANCES,
  CANVAS_RESET,
  INITIALISE_VISUALISER,
} from '@/components/shared/StoresActions';

import getters from '@/components/SchemaDesign/store/getters';
import mutations from '@/components/SchemaDesign/store/mutations';
import actions from '@/components/SchemaDesign/store/actions';
import state from '@/components/SchemaDesign/store/state';
import store from '@/store';


jest.mock('@/../Logger', () => ({ error: () => {} }));

jest.mock('@/components/shared/PersistentStorage', () => ({
  get: jest.fn(),
}));

jest.mock('@/components/ServerSettings', () => ({
  getServerHost: () => '127.0.0.1',
  getServerUri: () => '127.0.0.1:48555',
}));

Vue.use(Vuex);

jest.setTimeout(30000);

Array.prototype.flatMap = function

flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

let visFacade;

beforeAll(() => {
  store.registerModule('schema-design', { namespaced: true, getters, state, mutations, actions });

  store.dispatch('initGrakn');

  visFacade = {
    fitGraphToWindow: jest.fn(),
    addToCanvas: jest.fn(),
    resetCanvas: jest.fn(),
    getNode: jest.fn().mockImplementation(() => ['1234']),
    getAllNodes: jest.fn().mockImplementation(() => [
      { id: 123, baseType: 'ENTITY' },
      { id: 456, baseType: 'ATTRIBUTE' },
      { id: 789, baseType: 'RELATIONSHIP' },
    ]),
  };

  store.commit('schema-design/setVisFacade', visFacade);
});

describe('actions', () => {
  test('CURRENT_KEYSPACE_CHANGED', () => {
    store.dispatch(`schema-design/${CURRENT_KEYSPACE_CHANGED}`, 'gene');

    expect(store.state['schema-design'].currentKeyspace).toBe('gene');
    expect(store.state['schema-design'].graknSession).toBeDefined();
  });

  test('OPEN_GRAKN_TX', async () => {
    const graknTx = await store.dispatch(`schema-design/${OPEN_GRAKN_TX}`);

    expect(graknTx).toBeDefined();
    expect(store.state['schema-design'].schemaHandler).toBeDefined();
  });

  test('UPDATE_METATYPE_INSTANCES', async () => {
    await store.dispatch(`schema-design/${UPDATE_METATYPE_INSTANCES}`);

    expect(store.state['schema-design'].metaTypeInstances.entities).toHaveLength(1);
    expect(store.state['schema-design'].metaTypeInstances.attributes).toHaveLength(9);
    expect(store.state['schema-design'].metaTypeInstances.relationships).toHaveLength(6);
    expect(store.state['schema-design'].metaTypeInstances.roles).toHaveLength(23);
  });

  test('CANVAS_RESET', () => {
    store.commit('schema-design/selectedNodes', ['1234']);
    expect(store.state['schema-design'].selectedNodes).toEqual(['1234']);

    store.dispatch(`schema-design/${CANVAS_RESET}`);

    expect(store.state['schema-design'].selectedNodes).toBe(null);
    expect(visFacade.resetCanvas).toBeCalled();
  });
});

