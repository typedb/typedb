import DataManagementUtils from '@/components/DataManagement/DataManagementUtils';
import mockConcepts from '../../../helpers/MockConcepts.js';


Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

jest.mock('@/components/DataManagement/DataManagementContent/NodeSettingsPanel/NodeSettings', () => ({
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

describe('loadNeighbours', () => {
  test('schema concept', async () => {
    const data = await DataManagementUtils.loadNeighbours(mockConcepts.getMockEntityType(), 1, 0);
    expect(data.nodes).toHaveLength(1);
    expect(data.nodes[0].type).toBe('person');
    expect(data.nodes[0].label).toBe('person: 3333');
    expect(data.edges).toHaveLength(1);
    expect(data.edges[0].from).toBe('3333');
    expect(data.edges[0].to).toBe('0000');
    expect(data.edges[0].label).toBe('isa');
  });

  test('entity', async () => {
    const data = await DataManagementUtils.loadNeighbours(mockConcepts.getMockEntity1(), 1, 0);
    expect(data.nodes).toHaveLength(3);
    expect(data.nodes.filter(x => x.baseType === 'RELATIONSHIP')[0].id).toBe('6666');
    expect(data.nodes.filter(x => x.id === '3333')[0].label).toBe('person: 3333');
    expect(data.nodes.filter(x => x.id === '4444')[0].label).toBe('person: 4444');

    expect(data.edges).toHaveLength(2);
    expect(data.edges.filter(x => x.label === 'spouse1')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'spouse1')[0].to).toBe('3333');
    expect(data.edges.filter(x => x.label === 'spouse2')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'spouse2')[0].to).toBe('4444');
  });

  test('attribute', async () => {
    const data = await DataManagementUtils.loadNeighbours(mockConcepts.getMockAttribute(), 1, 0);
    expect(data.nodes).toHaveLength(1);
    expect(data.nodes[0].label).toBe('person: 3333');
    expect(data.edges).toHaveLength(1);
    expect(data.edges[0].label).toBe('has');
    expect(data.edges[0].from).toBe('3333');
    expect(data.edges[0].to).toBe('5555');
  });

  test('relationship', async () => {
    const data = await DataManagementUtils.loadNeighbours(mockConcepts.getMockRelationship(), 1, 0);
    expect(data.nodes).toHaveLength(1);
    expect(data.nodes.filter(x => x.id === '3333')[0].label).toBe('person: 3333');

    expect(data.edges).toHaveLength(1);
    expect(data.edges.filter(x => x.label === 'spouse1')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'spouse1')[0].to).toBe('3333');
  });
});
