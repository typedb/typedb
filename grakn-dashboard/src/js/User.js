import EngineClient from '../js/EngineClient.js';


//Default constant values
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
        localStorage.removeItem('id_token')
    },

    isAuthenticated() {
        let jwt = localStorage.getItem('id_token');
        if (jwt) {
            return true;
        } else {
            return false;
        }
    },

    // -------------- Reasoner and materialisation toggles ------------ //
    setReasonerStatus(status) {
        localStorage.setItem('use_reasoner', status);
    },

    //@return boolean
    getReasonerStatus() {
        let useReasoner = localStorage.getItem('use_reasoner');
        if (useReasoner == undefined) {
            this.setReasonerStatus(DEFAULT_USE_REASONER);
            return DEFAULT_USE_REASONER;
        } else {
            return (useReasoner === 'true');
        }
    },

    setMaterialiseStatus(status) {
        localStorage.setItem('reasoner_materialise', status);
    },

    //@return boolean
    getMaterialiseStatus() {
        let materialiseReasoner = localStorage.getItem('reasoner_materialise');
        if (materialiseReasoner == undefined) {
            this.setMaterialiseStatus(DEFAULT_MATERIALISE);
            return DEFAULT_MATERIALISE;
        } else {
            return (materialiseReasoner === 'true');
        }
    },


    // ---- Current Keyspace -----//
    setCurrentKeySpace(keyspace) {
        localStorage.setItem('current_keyspace', keyspace);
    },

    getCurrentKeySpace() {
        if (localStorage.getItem('current_keyspace') == undefined) this.setCurrentKeySpace(DEFAULT_KEYSPACE);
        return localStorage.getItem('current_keyspace');
    },
    // ------ Join-community modal setter and getter (has it already be shown to the user) ----//
    setModalShown(shown) {
        localStorage.setItem('community_modal', shown);
    },

    getModalShown() {
        let modalShown = localStorage.getItem('community_modal');
        if (modalShown == undefined) {
            this.setModalShown(false);
            return false;
        } else {
            return (modalShown === 'true');
        }
    }

}
