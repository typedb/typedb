import Vue from 'vue';
import Vuex from 'vuex';

import visTab from '@/components/Visualiser/VisTab';
import getters from '@/components/Visualiser/store/getters';
import mutations from '@/components/Visualiser/store/mutations';
import actions from '@/components/Visualiser/store/actions';

import TabState from '@/components/Visualiser/store/tabState';


import { shallowMount } from '@vue/test-utils';

jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));

jest.mock('@/../Logger', () => ({ error: () => {} }));

jest.mock('@/components/shared/PersistentStorage', () => {});

Vue.use(Vuex);

describe('tabs', () => {
  test('creating new visualiser tab component creates new module with state and getters', () => {
    const store = new Vuex.Store({
    });

    shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 0 },
    });

    expect(store.state['tab-0']).toBeDefined();
    expect(store.getters).toBeDefined();
  });

  test('creating new visualiser tab component', () => {
    const store = new Vuex.Store({
    });

    shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 0 },
    });

    store.commit('tab-0/currentKeyspace', 'gene');

    expect(store.getters['tab-0/currentKeyspace']).toBe('gene');
  });

  test('destroying a tab component removes respective namespace module', () => {
    const store = new Vuex.Store({
    });

    // first tab
    const tab0 = shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 0 },
    });

    expect(store.state['tab-0']).toBeDefined();

    tab0.destroy();

    // first tab
    shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 1 },
    });
    expect(store.state['tab-0']).not.toBeDefined();
    expect(store.state['tab-1']).toBeDefined();
  });

  test('dispatching actions only effects namespaced state', async () => {
    const store = new Vuex.Store({
      state: { grakn: { session: () => jest.fn() } },
    });

    shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 0 },
    });

    shallowMount(Object.assign({
      store,
    }, visTab), {
      propsData: { tabId: 1 },
    });

    store.commit('tab-0/setVisFacade', { resetCanvas: jest.fn(), getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]) });
    store.commit('tab-1/setVisFacade', { resetCanvas: jest.fn(), getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]) });

    expect(store.getters['tab-0/currentKeyspace']).toBe(null);
    expect(store.getters['tab-1/currentKeyspace']).toBe(null);

    await store.dispatch('tab-0/current-keyspace-changed', 'gene');

    expect(store.getters['tab-0/currentKeyspace']).toBe('gene');
    expect(store.getters['tab-1/currentKeyspace']).toBe(null);
  });

  test('unregistering a tab module renmoves namespace form store', async () => {
    const store = new Vuex.Store({
    });

    store.registerModule('tab-0', { namespaced: true, getters, state: TabState.create(), mutations, actions });
    store.registerModule('tab-1', { namespaced: true, getters, state: TabState.create(), mutations, actions });


    expect(store.state['tab-0']).toBeDefined();
    expect(store.state['tab-1']).toBeDefined();

    store.unregisterModule('tab-1');

    expect(store.state['tab-0']).toBeDefined();
    expect(store.state['tab-1']).not.toBeDefined();

    store.registerModule('tab-1', { namespaced: true, getters, state: TabState.create(), mutations, actions });

    expect(store.state['tab-0']).toBeDefined();
    expect(store.state['tab-1']).toBeDefined();
  });
});

