function getMockEntityType() {
  return {
    baseType: 'ENTITY_TYPE',
    id: '0000',
    label: () => Promise.resolve('person'),
    instances: () => Promise.resolve({ next: () => Promise.resolve(getMockEntity1()) }), // eslint-disable-line no-use-before-define
    isImplicit: () => Promise.resolve(false),
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()) }), // eslint-disable-line no-use-before-define
  };
}

function getMockAttributeType() {
  return {
    baseType: 'ATTRIBUTE_TYPE',
    id: '1111',
    label: () => Promise.resolve('name'),
    isImplicit: () => Promise.resolve(false),
  };
}

function getMockRelationshipType() {
  return {
    baseType: 'RELATIONSHIP_TYPE',
    id: '2222',
    label: () => Promise.resolve('marriage'),
    isImplicit: () => Promise.resolve(false),
  };
}

function getMockImplicitRelationshipType() {
  return {
    baseType: 'RELATIONSHIP_TYPE',
    id: '2222',
    isImplicit: () => Promise.resolve(true),
  };
}

function getMockEntity1() {
  return {
    baseType: 'ENTITY',
    id: '3333',
    type: () => Promise.resolve(getMockEntityType()),
    relationships: () => Promise.resolve({ next: () => Promise.resolve(getMockRelationship()) }), // eslint-disable-line no-use-before-define
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()) }), // eslint-disable-line no-use-before-define
  };
}

function getMockEntity2() {
  return {
    baseType: 'ENTITY',
    id: '4444',
    type: () => Promise.resolve(getMockEntityType()),
  };
}

function getMockAttribute() {
  return {
    baseType: 'ATTRIBUTE',
    id: '5555',
    type: () => Promise.resolve(getMockAttributeType()),
    value: () => Promise.resolve('John'),
    owners: () => Promise.resolve({ next: () => Promise.resolve(getMockEntity1()) }),
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()) }), // eslint-disable-line no-use-before-define
  };
}

function getMockRelationship() {
  const mockRole1 = { label: () => Promise.resolve('spouse1') };
  const mockRole2 = { label: () => Promise.resolve('spouse2') };
  const mockRolePlayers = new Map();
  mockRolePlayers.set(mockRole1, new Set([getMockEntity1()]));
  mockRolePlayers.set(mockRole2, new Set([getMockEntity2()]));

  return {
    baseType: 'RELATIONSHIP',
    id: '6666',
    type: () => Promise.resolve(getMockRelationshipType()),
    rolePlayersMap: () => Promise.resolve(mockRolePlayers),
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()) }), // eslint-disable-line no-use-before-define
  };
}

function getMockImplicitRelationship() {
  return {
    baseType: 'RELATIONSHIP',
    id: '6666',
    type: () => Promise.resolve(getMockImplicitRelationshipType()),
    isThing: () => true,
  };
}

export default {
  getMockEntityType,
  getMockAttributeType,
  getMockRelationshipType,
  getMockEntity1,
  getMockEntity2,
  getMockAttribute,
  getMockRelationship,
  getMockImplicitRelationship,
};

