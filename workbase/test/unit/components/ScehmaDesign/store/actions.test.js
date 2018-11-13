import Vue from 'vue';
import Vuex from 'vuex';

import {
  CURRENT_KEYSPACE_CHANGED,
} from '@/components/shared/StoresActions';

import actions from '@/components/SchemaDesign/store/actions';
import mutations from '@/components/SchemaDesign/store/mutations';
import getters from '@/components/SchemaDesign/store/getters';
import state from '@/components/SchemaDesign/store/state';


jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));

jest.mock('@/components/shared/PersistentStorage', () => {});


Vue.use(Vuex);

describe('actions', () => {
  test('LOAD_SCHEMA', () => {
    const store = new Vuex.Store({
      actions,
      mutations,
      getters,
      state,
    });

    store.dispatch(CURRENT_KEYSPACE_CHANGED, 'gene');
  });
});

