import EngineClient from '../js/EngineClient';


// Default constant values
const DEFAULT_USE_REASONER = false;
const DEFAULT_MATERIALISE = false;
const DEFAULT_KEYSPACE = 'grakn';

export default {

    // --------- User Login and Authentication ----------- //
  newSession(creds, fn) {
    EngineClient.newSession(creds, fn);
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

};
