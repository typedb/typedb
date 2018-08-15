import storage from '@/components/shared/PersistentStorage';

import Vue from 'vue';
import Vuex from 'vuex';
import createLogger from 'vuex/dist/logger';

import * as actions from './actions';
import * as getters from './getters';

Vue.use(Vuex);

const debug = process.env.NODE_ENV !== 'production';

export default new Vuex.Store({
  state: {
    grakn: undefined,
    keyspaces: undefined,
    credentials: undefined,
    isAuthenticated: undefined,
    landingPage: undefined,
    userLogged: false,
    isGraknRunning: undefined,
  },
  actions,
  getters,
  mutations: {
    setGrakn(state, grakn) {
      state.grakn = grakn;
    },
    setIsGraknRunning(state, isGraknRunning) {
      state.isGraknRunning = isGraknRunning;
    },
    setKeyspaces(state, list) {
      state.keyspaces = list;
    },
    setCredentials(state, credentials) {
      state.credentials = credentials;
    },
    setAuthentication(state, isAuthenticated) {
      state.isAuthenticated = isAuthenticated;
    },
    setLandingPage(state, landingPage) {
      state.landingPage = landingPage;
    },
    deleteCredentials(state) {
      state.credentials = null;
      storage.delete('user-credentials');
    },
    loadLocalCredentials(state, ENGINE_AUTHENTICATED) {
      if (!ENGINE_AUTHENTICATED) {
        state.credentials = null;
      } else {
        const localCredentials = storage.get('user-credentials');
        state.credentials = (localCredentials) ? JSON.parse(localCredentials) : null;
        state.userLogged = (state.credentials);
      }
    },
    userLogged(state, logged) {
      state.userLogged = logged;
    },
  },
  strict: debug,
  plugins: debug ? [createLogger()] : [],
});

