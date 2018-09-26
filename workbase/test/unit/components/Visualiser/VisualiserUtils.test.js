import VisualiserUtils from '@/components/Visualiser/VisualiserUtils.js';
import mockConcepts from '../../../helpers/MockConcepts';

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

jest.mock('@/components/Visualiser/RightBar/SettingsTab/DisplaySettings.js', () => ({
  getTypeLabels() { return []; },
}));

jest.mock('@/components/shared/PersistentStorage', () => ({
  get() { return 10; },
}));

describe('limit Query', () => {
  test('add offset and limit to query', () => {
    const query = 'match $x isa person; get;';
    const limited = VisualiserUtils.limitQuery(query);
    expect(limited).toBe('match $x isa person; offset 0; limit 10; get;');
  });
  test('add offset to query already containing limit', () => {
    const query = 'match $x isa person; limit 40; get;';
    const limited = VisualiserUtils.limitQuery(query);
    expect(limited).toBe('match $x isa person; limit 40; offset 0; get;');
  });
  test('add limit to query already containing offset', () => {
    const query = 'match $x isa person; offset 20; get;';
    const limited = VisualiserUtils.limitQuery(query);
    expect(limited).toBe('match $x isa person; offset 20; limit 10; get;');
  });
  test('query already containing offset and limit does not get changed', () => {
    const query = 'match $x isa person; offset 0; limit 40; get;';
    const limited = VisualiserUtils.limitQuery(query);
    expect(limited).toBe(query);
  });
  test('query already containing offset and limit in inverted order does not get changed', () => {
    const query = 'match $x isa person; limit 40; offset 0; get;';
    const limited = VisualiserUtils.limitQuery(query);
    expect(limited).toBe(query);
  });
  test('query containing multi-line queries', () => {
    const query = `
    match $x isa person;
    $r($x, $y); get;`;
    const limited = VisualiserUtils.limitQuery(query);
    expect(limited).toBe(`
    match $x isa person;
    $r($x, $y); offset 0; limit 10; get;`);
  });
});

describe('Get Neighbours Query', () => {
  test('Type', () => {
    const neighboursQuery = VisualiserUtils.getNeighboursQuery(mockConcepts.getMockEntityType(), 1);
    expect(neighboursQuery).toBe('match $x id "0000"; $y isa $x; offset 0; limit 1; get $y;');
  });
  test('Entity', () => {
    const neighboursQuery = VisualiserUtils.getNeighboursQuery(mockConcepts.getMockEntity1(), 1);
    expect(neighboursQuery).toBe('match $x id "3333"; $r ($x, $y); offset 0; limit 1; get $r, $y;');
  });
  test('Attribute', () => {
    const neighboursQuery = VisualiserUtils.getNeighboursQuery(mockConcepts.getMockAttribute(), 1);
    expect(neighboursQuery).toBe('match $x has attribute $y; $y id "5555"; offset 0; limit 1; get $x;');
  });
  test('Relationship', () => {
    const neighboursQuery = VisualiserUtils.getNeighboursQuery(mockConcepts.getMockRelationship(), 1);
    expect(neighboursQuery).toBe('match $r id "6666"; $r ($x, $y); offset 0; limit 1; get $x;');
  });
});

describe('Compute Attributes', () => {
  test('attach attributes to type', async () => {
    const nodes = await VisualiserUtils.computeAttributes([mockConcepts.getMockEntityType()]);
    expect(nodes[0].attributes[0].type).toBe('name');
  });
  test('attach attributes to thing', async () => {
    const nodes = await VisualiserUtils.computeAttributes([mockConcepts.getMockEntity1()]);
    expect(nodes[0].attributes[0].type).toBe('name');
    expect(nodes[0].attributes[0].value).toBe('John');
  });
});

describe('Build Explanation Query', () => {
  test('attach attributes to type', async () => {
  });
});
