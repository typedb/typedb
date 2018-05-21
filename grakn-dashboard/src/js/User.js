import EngineClient from './EngineClient';


// Default constant values
const DEFAULT_USE_REASONER = true;
const DEFAULT_ROLE_PLAYERS = true;
export const DEFAULT_KEYSPACE = 'grakn';
const DEFAULT_QUERY_LIMIT = '30';
const DEFAULT_ROLE_PLAYERS_LIMIT = '50';



export default {

  // -------------- Reasoner toggles ------------ //
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

    // -------------- Relationship Settings ------------ //

    setRolePlayersStatus(status) {
      localStorage.setItem('load_role_players', status);
    },
  
    // @return boolean
    getRolePlayersStatus() {
      const rolePlayers = localStorage.getItem('load_role_players');
      if (rolePlayers == null) {
        this.setRolePlayersStatus(DEFAULT_ROLE_PLAYERS);
        return DEFAULT_ROLE_PLAYERS;
      }
      return (rolePlayers === 'true');
    },

    setRolePlayersLimit(value) {
      let parsedValue = parseInt(value, 10) || 0;
      if (parsedValue < 0) parsedValue = 0;
      localStorage.setItem('role_players_limit', parsedValue);
    },

    getRolePlayersLimit() {
      const rolePlayersLimit = localStorage.getItem('role_players_limit');
      if (rolePlayersLimit == null) {
        this.setRolePlayersLimit(DEFAULT_ROLE_PLAYERS_LIMIT);
        return DEFAULT_ROLE_PLAYERS_LIMIT;
      }
      return rolePlayersLimit;
    },

  // ---- Current Keyspace -----//
  setCurrentKeySpace(keyspace) {
    localStorage.setItem('current_keyspace', keyspace);
  },

  getCurrentKeySpace() {
    if (!localStorage.getItem('current_keyspace')) {
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
