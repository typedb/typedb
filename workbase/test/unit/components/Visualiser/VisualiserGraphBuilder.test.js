import VisualiserGraphBuilder from '@/components/Visualiser/VisualiserGraphBuilder.js';
import mockConcepts from '../../../helpers/MockConcepts';

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

jest.mock('@/components/Visualiser/RightBar/SettingsTab/DisplaySettings.js', () => ({
  getTypeLabels() { return []; },
}));

jest.mock('@/components/shared/PersistentStorage', () => ({
  get() { return 10; },
}));

describe('prepareNodes', () => {
  test('schema concept', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([mockConcepts.getMockEntityType()]);
    expect(prepared[0].label).toBe('person');
  });
  test('entity', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([mockConcepts.getMockEntity1()]);
    expect(prepared[0].type).toBe('person');
    expect(prepared[0].label).toBe('person: 3333');
  });
  test('attribute', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([mockConcepts.getMockAttribute()]);
    expect(prepared[0].type).toBe('name');
    expect(prepared[0].value).toBe('John');
    expect(prepared[0].label).toBe('name: John');
  });
  test('relationship', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([mockConcepts.getMockRelationship()]);
    expect(prepared[0].type).toBe('parentship');
  });
});


describe('relationshipsRolePlayers', () => {
  test('loadRelationshipsRolePlayers', async () => {
    const relationships = [];
    relationships.push(mockConcepts.getMockRelationship());
    const roleplayers = await VisualiserGraphBuilder.relationshipsRolePlayers(relationships);

    expect(roleplayers.nodes).toHaveLength(2);
    expect(roleplayers.nodes.filter(x => x.id === '3333')[0].label).toBe('person: 3333');
    expect(roleplayers.nodes.filter(x => x.id === '4444')[0].label).toBe('person: 4444');

    expect(roleplayers.edges).toHaveLength(2);
    expect(roleplayers.edges.filter(x => x.label === 'spouse1')[0].from).toBe('6666');
    expect(roleplayers.edges.filter(x => x.label === 'spouse1')[0].to).toBe('3333');
    expect(roleplayers.edges.filter(x => x.label === 'spouse2')[0].from).toBe('6666');
    expect(roleplayers.edges.filter(x => x.label === 'spouse2')[0].to).toBe('4444');
  });
});

// describe('loadAttribtues', () => {
//   test('schema concept', async () => {
//     const data = await VisualiserGraphBuilder.loadAttributes(mockConcepts.getMockEntityType(), 1, 0);
//     expect(data.nodes).toHaveLength(1);
//     expect(data.nodes[0].type).toBe('name');
//     expect(data.nodes[0].label).toBe('name: John');
//     expect(data.edges).toHaveLength(1);
//     expect(data.edges[0].from).toBe('0000');
//     expect(data.edges[0].to).toBe('5555');
//     expect(data.edges[0].label).toBe('has');
//   });
//   test('entity', async () => {
//     const data = await VisualiserGraphBuilder.loadAttributes(mockConcepts.getMockEntity1(), 1, 0);
//     expect(data.nodes).toHaveLength(1);
//     expect(data.nodes[0].type).toBe('name');
//     expect(data.nodes[0].label).toBe('name: John');
//     expect(data.edges).toHaveLength(1);
//     expect(data.edges[0].from).toBe('3333');
//     expect(data.edges[0].to).toBe('5555');
//     expect(data.edges[0].label).toBe('has');
//   });
//   test('attribute', async () => {
//     const data = await VisualiserGraphBuilder.loadAttributes(mockConcepts.getMockAttribute(), 1, 0);
//     expect(data.nodes).toHaveLength(1);
//     expect(data.nodes[0].type).toBe('name');
//     expect(data.nodes[0].label).toBe('name: John');
//     expect(data.edges).toHaveLength(1);
//     expect(data.edges[0].from).toBe('5555');
//     expect(data.edges[0].to).toBe('5555');
//     expect(data.edges[0].label).toBe('has');
//   });
//   test('relationship', async () => {
//     const data = await VisualiserGraphBuilder.loadAttributes(mockConcepts.getMockRelationship(), 1, 0);
//     expect(data.nodes).toHaveLength(1);
//     expect(data.nodes[0].type).toBe('name');
//     expect(data.nodes[0].label).toBe('name: John');
//     expect(data.edges).toHaveLength(1);
//     expect(data.edges[0].from).toBe('6666');
//     expect(data.edges[0].to).toBe('5555');
//     expect(data.edges[0].label).toBe('has');
//   });
// });
