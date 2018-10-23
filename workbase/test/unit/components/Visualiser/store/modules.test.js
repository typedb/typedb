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

  test('destroying a tab component removes respective namespace', () => {
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
});

