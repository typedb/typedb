/*-
 * #%L
 * grakn-dashboard
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
import EngineClient from './EngineClient';


// Default constant values
const DEFAULT_USE_REASONER = true;
export const DEFAULT_KEYSPACE = 'grakn';
const DEFAULT_QUERY_LIMIT = '30';


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
