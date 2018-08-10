import NodeSettings from '@/components/DataManagement/NodeSettings';
import PersistentStorage from '@/components/shared/PersistentStorage';

jest.mock('@/components/shared/PersistentStorage', () => ({
  get: jest.fn(),
  set: jest.fn(),
}));

const keyspacePreferences = '{"test-keyspace":{"label":{"person":["name"]},"colour":{"person":"#b3a41e"},"position":{}}}';

PersistentStorage.get.mockImplementation((key) => {
  if (key === 'keyspaces-preferences') {
    return keyspacePreferences;
  }
  return 'test-keyspace';
});

describe('Labels', () => {
  // Check if entity 'person' has attribute 'name' in its label
  test('getTypeLabels', () => {
    const mockType = 'person';
    const labels = NodeSettings.getTypeLabels(mockType);
    expect(labels).toEqual(['name']);
  });

  // Check if attribute 'age' is added to the label of entity 'person'
  test('add attribute to type label', () => {
    const expectedMapString = '{"test-keyspace":{"label":{"person":["name","age"]},"colour":{"person":"#b3a41e"},"position":{}}}';

    const mockAttribute = 'age';
    const mockType = 'person';
    NodeSettings.toggleLabelByType({ type: mockType, attribute: mockAttribute });
    expect(PersistentStorage.set).toBeCalledWith('keyspaces-preferences', expectedMapString);
  });

  // Check if attribute 'age' is removed from the label of entity 'person'
  test('remove attribute from type label', () => {
    const expectedMapString = '{"test-keyspace":{"label":{"person":[]},"colour":{"person":"#b3a41e"},"position":{}}}';

    const mockAttribute = 'name';
    const mockType = 'person';
    NodeSettings.toggleLabelByType({ type: mockType, attribute: mockAttribute });
    expect(PersistentStorage.set).toBeCalledWith('keyspaces-preferences', expectedMapString);
  });
});

describe('Colours', () => {
  // Check if the node of entity 'person' has color '#b3a41e'
  test('getTypeColours', () => {
    const mockType = 'person';
    const labels = NodeSettings.getTypeColours(mockType);
    expect(labels).toEqual('#b3a41e');
  });

  // Check if colour '#279d5d' is added to the node of entity 'person'
  test('toggleColourByType', () => {
    const expectedMapString = '{"test-keyspace":{"label":{"person":["name"]},"colour":{"person":"#279d5d"},"position":{}}}';

    const mockColour = '#279d5d';
    const mockType = 'person';
    NodeSettings.toggleColourByType({ type: mockType, colourString: mockColour });
    expect(PersistentStorage.set).toBeCalledWith('keyspaces-preferences', expectedMapString);
  });

  // Check if the node of entity 'person' has no colour (defaut colour will be set)
  test('reset node colour', () => {
    const expectedMapString = '{"test-keyspace":{"label":{"person":["name"]},"colour":{},"position":{}}}';

    const mockType = 'person';
    NodeSettings.toggleColourByType({ type: mockType });
    expect(PersistentStorage.set).toBeCalledWith('keyspaces-preferences', expectedMapString);
  });
});
