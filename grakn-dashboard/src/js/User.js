import EngineClient from './EngineClient';


// Default constant values
const DEFAULT_USE_REASONER = false;
const DEFAULT_MATERIALISE = false;
export const DEFAULT_KEYSPACE = 'grakn';
const DEFAULT_QUERY_LIMIT = '30';


export default {

    // --------- User Login and Authentication ----------- //
  newSession(creds, fn) {
    return EngineClient.newSession(creds);
  },

  setAuthToken(token) {
    localStorage.setItem('id_token', token);
  },

  logout() {
    localStorage.removeItem('id_token');
  },

  isAuthenticated() {
    const jwt = localStorage.getItem('id_token');
    if (jwt) return true;
    return false;
  },

    // -------------- Reasoner and materialisation toggles ------------ //
  setReasonerStatus(status) {
    localStorage.setItem('use_reasoner', status);
  },

    // @return boolean
  getReasonerStatus() {
    const useReasoner = localStorage.getItem('use_reasoner');
    if (useReasoner == null) {
      this.setReasonerStatus(DEFAULT_USE_REASONER);
      return DEFAULT_USE_REASONER;
    }
    return (useReasoner === 'true');
  },

  setMaterialiseStatus(status) {
    localStorage.setItem('reasoner_materialise', status);
  },

  getFreezeNodes() {
    const freezeNodes = localStorage.getItem('freeze_nodes');
    if (freezeNodes == null) {
      this.setFreezeNodes(false);
      return false;
    }
    return (freezeNodes === 'true');
  },

  setFreezeNodes(status) {
    localStorage.setItem('freeze_nodes', status);
  },

    // @return boolean
  getMaterialiseStatus() {
    const materialiseReasoner = localStorage.getItem('reasoner_materialise');
    if (materialiseReasoner == null) {
      this.setMaterialiseStatus(DEFAULT_MATERIALISE);
      return DEFAULT_MATERIALISE;
    }
    return (materialiseReasoner === 'true');
  },

    // ---- Current Keyspace -----//
  setCurrentKeySpace(keyspace) {
    localStorage.setItem('current_keyspace', keyspace);
  },

  getCurrentKeySpace() {
    if (!localStorage.getItem('current_keyspace')) {
      //  TODO: check if the default keyspace has been created on server side.
      this.setCurrentKeySpace(DEFAULT_KEYSPACE);
    }
    return localStorage.getItem('current_keyspace');
  },
    // ------ Join-community modal setter and getter (has it already be shown to the user) ----//
  setModalShown(shown) {
    localStorage.setItem('community_modal', shown);
  },

  getModalShown() {
    const modalShown = localStorage.getItem('community_modal');
    if (!modalShown) {
      this.setModalShown(false);
      return false;
    }
    return (modalShown === 'true');
  },

  // ------------ Limit number of results ---------------- //
  getQueryLimit() {
    const queryLimit = localStorage.getItem('query_limit');
    if (queryLimit == null) {
      this.setQueryLimit(DEFAULT_QUERY_LIMIT);
      return DEFAULT_QUERY_LIMIT;
    }
    return queryLimit;
  },
  setQueryLimit(value) {
    let parsedValue = parseInt(value, 10) || 0;
    if (parsedValue < 0) parsedValue = 0;
    localStorage.setItem('query_limit', parsedValue);
  },

};
