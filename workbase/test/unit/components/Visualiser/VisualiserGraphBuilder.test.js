import VisualiserGraphBuilder from '@/components/Visualiser/VisualiserGraphBuilder.js';
import MockConcepts from '../../../helpers/MockConcepts';

Array.prototype.flatMap = function flat(lambda) { return Array.prototype.concat.apply([], this.map(lambda)); };

jest.mock('@/components/Visualiser/RightBar/SettingsTab/DisplaySettings.js', () => ({
  getTypeLabels() { return []; },
}));

jest.mock('@/components/shared/PersistentStorage', () => ({
  get() { return 10; },
}));

describe('prepareNodes', () => {
  test('schema concept', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([MockConcepts.getMockEntityType()]);
    expect(prepared[0].label).toBe('person');
  });
  test('entity', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([MockConcepts.getMockEntity1()]);
    expect(prepared[0].type).toBe('person');
    expect(prepared[0].label).toBe('person: 3333');
  });
  test('attribute', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([MockConcepts.getMockAttribute()]);
    expect(prepared[0].type).toBe('name');
    expect(prepared[0].value).toBe('John');
    expect(prepared[0].label).toBe('name: John');
  });
  test('relationship', async () => {
    const prepared = await VisualiserGraphBuilder.prepareNodes([MockConcepts.getMockRelationship()]);
    expect(prepared[0].type).toBe('parentship');
  });
});

describe('relationshipsRolePlayers', () => {
  test('loadRelationshipsRolePlayers', async () => {
    const relationships = [];
    relationships.push(MockConcepts.getMockRelationship());
    const roleplayers = await VisualiserGraphBuilder.relationshipsRolePlayers(relationships);

    expect(roleplayers.nodes).toHaveLength(2);
    expect(roleplayers.nodes.filter(x => x.id === '3333')[0].label).toBe('person: 3333');
    expect(roleplayers.nodes.filter(x => x.id === '4444')[0].label).toBe('person: 4444');

    expect(roleplayers.edges).toHaveLength(2);
    expect(roleplayers.edges.filter(x => x.label === 'son')[0].from).toBe('6666');
    expect(roleplayers.edges.filter(x => x.label === 'son')[0].to).toBe('3333');
    expect(roleplayers.edges.filter(x => x.label === 'father')[0].from).toBe('6666');
    expect(roleplayers.edges.filter(x => x.label === 'father')[0].to).toBe('4444');
  });
});

describe('buildFromConceptMap', () => {
  test('buildFromConceptMap', async () => {
    const result = [MockConcepts.getMockAnswer2(), MockConcepts.getMockAnswerContainingRelationship()];

    const data = await VisualiserGraphBuilder.buildFromConceptMap(result, true, 20);

    expect(data.nodes).toHaveLength(6);
    expect(data.edges).toHaveLength(4);

    expect(data.nodes[0].explanation).toBe('mockExplanation');
    expect(data.nodes[1].explanation).toBe('mockExplanation');
    expect(data.nodes[2].explanation).toBe('mockExplanation');
    expect(data.nodes[3].explanation).toBe('mockExplanation');

    expect(data.nodes[1].label).toBeDefined();
    expect(data.nodes[2].label).toBeDefined();
    expect(data.nodes[3].label).toBeDefined();
    expect(data.nodes[4].label).toBeDefined();
    expect(data.nodes[5].label).toBeDefined();

    expect(data.edges.filter(x => x.label === 'son')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'son')[0].to).toBe('3333');
    expect(data.edges.filter(x => x.label === 'father')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'father')[0].to).toBe('4444');
  });
});

describe('buildFromConceptList', () => {
  test('buildFromConceptList', async () => {
    const mockPath = { list: () => ['3333', '6666', '4444'], explanation: () => null };
    const mockPathNodes = [MockConcepts.getMockEntity1(), MockConcepts.getMockRelationship(), MockConcepts.getMockEntity2()];

    const data = await VisualiserGraphBuilder.buildFromConceptList(mockPath, mockPathNodes);

    expect(data.nodes).toHaveLength(5);
    expect(data.nodes[0].id).toBe('6666');
    expect(data.nodes[1].id).toBe('3333');
    expect(data.nodes[2].id).toBe('4444');

    expect(data.edges).toHaveLength(2);
    expect(data.edges.filter(x => x.label === 'son')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'son')[0].to).toBe('3333');
    expect(data.edges.filter(x => x.label === 'father')[0].from).toBe('6666');
    expect(data.edges.filter(x => x.label === 'father')[0].to).toBe('4444');
  });
});
