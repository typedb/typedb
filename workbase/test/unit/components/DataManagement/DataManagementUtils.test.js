import DataManagementUtils from '@/components/Visualiser/VisualiserUtils.js';


Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

jest.mock('@/components/Visualiser/RightBar/SettingsTab/DisplaySettings.js', () => ({
  getTypeLabels() { return []; },
}));

jest.mock('@/components/shared/PersistentStorage', () => ({
  get() { return 10; },
}));

describe('limitQuery', () => {
  test('add offset and limit to query', () => {
    const query = 'match $x isa person; get;';
    const limited = DataManagementUtils.limitQuery(query);
    expect(limited).toBe('match $x isa person; offset 0; limit 10; get;');
  });
  test('add offset to query already containing limit', () => {
    const query = 'match $x isa person; limit 40; get;';
    const limited = DataManagementUtils.limitQuery(query);
    expect(limited).toBe('match $x isa person; limit 40; offset 0; get;');
  });
  test('add limit to query already containing offset', () => {
    const query = 'match $x isa person; offset 20; get;';
    const limited = DataManagementUtils.limitQuery(query);
    expect(limited).toBe('match $x isa person; offset 20; limit 10; get;');
  });
  test('query already containing offset and limit does not get changed', () => {
    const query = 'match $x isa person; offset 0; limit 40; get;';
    const limited = DataManagementUtils.limitQuery(query);
    expect(limited).toBe(query);
  });
  test('query already containing offset and limit in inverted order does not get changed', () => {
    const query = 'match $x isa person; limit 40; offset 0; get;';
    const limited = DataManagementUtils.limitQuery(query);
    expect(limited).toBe(query);
  });
  test('query containing multi-line queries', () => {
    const query = `
    match $x isa person; 
    $r($x, $y); get;`;
    const limited = DataManagementUtils.limitQuery(query);
    expect(limited).toBe(`
    match $x isa person; 
    $r($x, $y); offset 0; limit 10; get;`);
  });
});
