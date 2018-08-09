import QuerySettings from '@/components/DataManagement/QuerySettings';
import PersistentStorage from '@/components/shared/PersistentStorage';

jest.mock('@/components/shared/PersistentStorage', () => ({
  get: jest.fn(),
  set: jest.fn(),
}));

// Mock getter behaviour to return anything that has been set by the setter
PersistentStorage.set.mockImplementation((key, value) => {
  PersistentStorage.get.mockImplementation(() => value);
});

describe('LimitQuerySettings', () => {
  const DEFAULT_QUERY_LIMIT = '30';

  test('getQueryLimitDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const queryLimit = QuerySettings.getQueryLimit();
    expect(queryLimit).toBe(DEFAULT_QUERY_LIMIT);
  });

  test('getQueryLimitNotDefault', () => {
    PersistentStorage.set('limit', 10);
    const queryLimit = QuerySettings.getQueryLimit();
    expect(queryLimit).toBe(10);
  });

  test('setQueryLimit', () => {
    QuerySettings.setQueryLimit(5);
    expect(PersistentStorage.get()).toBe(5);
  });
});

describe('RelationshipSettings', () => {
  const DEFAULT_ROLE_PLAYERS = true;
  const DEFAULT_NEIGHBOURS_LIMIT = 20;

  test('setRolePlayersStatus', () => {
    QuerySettings.setRolePlayersStatus(true);
    expect(PersistentStorage.get()).toBe(true);
  });

  test('getRolePlayersStatusDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const rolePlayerStatus = QuerySettings.getRolePlayersStatus();
    expect(rolePlayerStatus).toBe(DEFAULT_ROLE_PLAYERS);
  });

  test('getRolePlayersStatusNotDefault', () => {
    PersistentStorage.set('status', false);
    const rolePlayerStatus = QuerySettings.getRolePlayersStatus();
    expect(rolePlayerStatus).toBe(false);
  });

  test('setNeighboursLimit', () => {
    QuerySettings.setNeighboursLimit(2);
    expect(PersistentStorage.get()).toBe(2);
  });

  test('getNeighboursLimitDefault', () => {
    PersistentStorage.get.mockImplementation(() => null);
    const neighboursLimit = QuerySettings.getNeighboursLimit();
    expect(neighboursLimit).toBe(DEFAULT_NEIGHBOURS_LIMIT);
  });

  test('getNeighboursLimitNotDefault', () => {
    PersistentStorage.get.mockImplementation(() => 5);
    const neighboursLimit = QuerySettings.getNeighboursLimit();
    expect(neighboursLimit).toBe(5);
  });
});
