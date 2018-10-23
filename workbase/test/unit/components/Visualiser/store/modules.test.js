import Vue from 'vue';
import Vuex from 'vuex';

import visTab from '@/components/Visualiser/VisTab';

import { shallowMount } from '@vue/test-utils';
import mutations from '@/components/Visualiser/store/mutations';
import getters from '@/components/Visualiser/store/getters';


jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));

jest.mock('@/../Logger', () => ({ error: () => {} }));

jest.mock('@/components/shared/PersistentStorage', () => {});

Vue.use(Vuex);

describe('tabs', () => {
  // test('creating new visualiser component creates new data object', () => {
  //   const store = new Vuex.Store({
  //     state: { tabs: {} },
  //     mutations,
  //   });

  //   shallowMount(Object.assign({
  //     store,
  //   }, visTab), {
  //     propsData: { tabId: 0 },
  //   });

  //   expect(store.state.tabs[0]).toBeDefined();
  // });
  // test('creating new visualiser component creates new data object with state', () => {
  //   const store = new Vuex.Store({
  //     state: { tabs: {} },
  //     mutations,
  //     getters,
  //   });

  //   shallowMount(Object.assign({
  //     store,
  //   }, visTab), {
  //     propsData: { tabId: 0 },
  //   });

  //   expect(store.getters.currentQuery(0)).toBe('');
  // });

  // test('creating new visualiser component creates new data object with an independent state', () => {
  //   const store = new Vuex.Store({
  //     state: { tabs: {} },
  //     mutations,
  //     getters,
  //   });

  //   // first tab
  //   shallowMount(Object.assign({
  //     store,
  //   }, visTab), {
  //     propsData: { tabId: 0 },
  //   });

  //   expect(store.getters.currentQuery(0)).toBe('');

  //   store.commit('currentQuery', { id: 0, query: 'A' });

  //   expect(store.getters.currentQuery(0)).toBe('A');

  //   // second tab
  //   shallowMount(Object.assign({
  //     store,
  //   }, visTab), {
  //     propsData: { tabId: 1 },
  //   });

  //   expect(store.getters.currentQuery(1)).toBe('');

  //   store.commit('currentQuery', { id: 1, query: 'B' });

  //   debugger;

  //   expect(store.getters.currentQuery(1)).toBe('B');
  // });

  test('test rectivity of getters', () => {
    const store = new Vuex.Store({
    });

    const visTab0 = shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 0 },
    });

    store.commit('tab-0/currentKeyspace', 'gene');

    expect(visTab0.vm.currentKeyspace).toBe('gene');
  });
});

