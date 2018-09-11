import storage from '@/components/shared/PersistentStorage';

// ------------ Limit number of results ---------------- //
const DEFAULT_QUERY_LIMIT = '30';

function getQueryLimit() {
  const queryLimit = storage.get('query_limit');
  if (queryLimit == null) {
    this.setQueryLimit(DEFAULT_QUERY_LIMIT);
    return DEFAULT_QUERY_LIMIT;
  }
  return queryLimit;
}

function setQueryLimit(value) {
  let parsedValue = parseInt(value, 10) || 0;
  if (parsedValue < 0) parsedValue = 0;
  storage.set('query_limit', parsedValue);
}

// -------------- Relationship Settings ------------ //

const DEFAULT_ROLE_PLAYERS = true;

function setRolePlayersStatus(status) {
  storage.set('load_role_players', status);
}

function getRolePlayersStatus() {
  const rolePlayers = storage.get('load_role_players');
  if (rolePlayers == null) {
    this.setRolePlayersStatus(DEFAULT_ROLE_PLAYERS);
    return DEFAULT_ROLE_PLAYERS;
  }
  return rolePlayers;
}

// -------------- Neighbor Settings ------------ //

const DEFAULT_NEIGHBOUR_LIMIT = 20;

function setNeighboursLimit(value) {
  let parsedValue = parseInt(value, 10) || 0;
  if (parsedValue < 0) parsedValue = 0;
  storage.set('neighbours_limit', parsedValue);
}

function getNeighboursLimit() {
  const neighbourLimit = storage.get('neighbours_limit');
  if (neighbourLimit == null) {
    this.setNeighboursLimit(DEFAULT_NEIGHBOUR_LIMIT);
    return DEFAULT_NEIGHBOUR_LIMIT;
  }
  return neighbourLimit;
}

export default {
  getQueryLimit,
  setQueryLimit,
  setRolePlayersStatus,
  getRolePlayersStatus,
  setNeighboursLimit,
  getNeighboursLimit,
  DEFAULT_QUERY_LIMIT,
};
