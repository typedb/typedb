import Vue from 'vue';
import Vuex from 'vuex';

import {
  LOAD_SCHEMA,
} from '@/components/shared/StoresActions';


import actions from '@/components/SchemaDesign/store/actions';
import mutations from '@/components/SchemaDesign/store/mutations';
import getters from '@/components/SchemaDesign/store/getters';

import MockConcepts from '../../../../helpers/MockConcepts';


jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));

jest.mock('@/components/shared/PersistentStorage', () => {});


Vue.use(Vuex);

describe('actions', () => {
  test('LOAD_SCHEMA', () => {
    const store = new Vuex.Store({
      actions,
      mutations,
      getters,
      state: {
        visFacade: {},
        graknSession: {
          transaction: () => Promise.resolve({ query: () => Promise.resolve({ collect: () => Promise.resolve([MockConcepts.getMockAnswer2()]) }) }),
        },
      },
    });

    store.dispatch(LOAD_SCHEMA);
  });
});

