function getMockEntityType() {
  return {
    baseType: 'ENTITY_TYPE',
    id: '0000',
    label: () => Promise.resolve('person'),
    instances: () => Promise.resolve({ next: () => Promise.resolve(getMockEntity1()) }), // eslint-disable-line no-use-before-define
    isImplicit: () => Promise.resolve(false),
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttributeType()), collect: () => Promise.resolve([getMockAttributeType()]) }), // eslint-disable-line no-use-before-define
    isType: () => true,
    offset: 0,
  };
}

function getMockAttributeType() {
  return {
    baseType: 'ATTRIBUTE_TYPE',
    id: '1111',
    label: () => Promise.resolve('name'),
    isImplicit: () => Promise.resolve(false),
    isType: () => true,
  };
}

function getMockRelationshipType() {
  return {
    baseType: 'RELATIONSHIP_TYPE',
    id: '2222',
    label: () => Promise.resolve('parentship'),
    isImplicit: () => Promise.resolve(false),
  };
}

function getMockImplicitRelationshipType() {
  return {
    baseType: 'RELATIONSHIP_TYPE',
    id: '2222',
    isImplicit: () => Promise.resolve(true),
    isThing: () => false,
  };
}

function getMockEntity1() {
  return {
    baseType: 'ENTITY',
    id: '3333',
    type: () => Promise.resolve(getMockEntityType()),
    relationships: () => Promise.resolve({ next: () => Promise.resolve(getMockRelationship()) }), // eslint-disable-line no-use-before-define
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()), collect: () => Promise.resolve([getMockAttribute()]) }), // eslint-disable-line no-use-before-define
    isType: () => false,
    isInferred: () => Promise.resolve(false),
    isAttribute: () => false,
    isEntity: () => true,
    isThing: () => true,
    offset: 0,
  };
}

function getMockEntity2() {
  return {
    baseType: 'ENTITY',
    id: '4444',
    type: () => Promise.resolve(getMockEntityType()),
    isAttribute: () => false,
    isEntity: () => true,
    isThing: () => true,
    isInferred: () => Promise.resolve(false),
    isRelationship: () => false,
  };
}

function getMockAttribute() {
  return {
    baseType: 'ATTRIBUTE',
    id: '5555',
    type: () => Promise.resolve(getMockAttributeType()),
    value: () => Promise.resolve('John'),
    owners: () => Promise.resolve({ next: () => Promise.resolve(getMockEntity1()) }),
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()), collect: () => Promise.resolve([getMockAttribute()]) }), // eslint-disable-line no-use-before-define
    isType: () => false,
    isInferred: () => Promise.resolve(false),
    isAttribute: () => true,
    isThing: () => true,
    offset: 0,
  };
}

function getMockRelationship() {
  const mockRole1 = { label: () => Promise.resolve('son') };
  const mockRole2 = { label: () => Promise.resolve('father') };
  const mockRolePlayers = new Map();
  mockRolePlayers.set(mockRole1, new Set([getMockEntity1()]));
  mockRolePlayers.set(mockRole2, new Set([getMockEntity2()]));

  return {
    baseType: 'RELATIONSHIP',
    id: '6666',
    type: () => Promise.resolve(getMockRelationshipType()),
    rolePlayersMap: () => Promise.resolve(mockRolePlayers),
    attributes: () => Promise.resolve({ next: () => Promise.resolve(getMockAttribute()), collect: () => Promise.resolve([getMockAttribute()]) }), // eslint-disable-line no-use-before-define
    isType: () => false,
    isInferred: () => Promise.resolve(false),
    isAttribute: () => false,
    isEntity: () => false,
    isThing: () => true,
    isRelationship: () => true,
    offset: 0,
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

function getMockAnswer1() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('p', getMockEntity1());
      map.set('c1', getMockEntity2());
      return map;
    },
  };
}

const getMockQueryPattern1 = '{(child: $c1, parent: $p) isa parentship;}';

function getMockAnswer2() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('c', getMockEntity1());
      map.set('1234', getMockAttribute());
      return map;
    },
  };
}

const getMockQueryPattern2 = '{$1234 "male"; $c has gender $1234; $c id V4444;}';

function getMockAnswer3() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('p', getMockEntity1());
      map.set('c', getMockEntity2());
      map.set('1234', getMockRelationship());
      return map;
    },
  };
}

const getMockQueryPattern3 = '{$c id 4444; $p id 3333; $1234 (child: $c, parent: $p) isa parentship;}';

function getMockAnswerContainingImplicitType() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('p', getMockEntity1());
      map.set('c', getMockEntity2());
      map.set('r', getMockImplicitRelationship());
      return map;
    },
  };
}

function getMockAnswerContainingRelationship() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('r', getMockRelationship());
      map.set('c', getMockEntity2());
      return map;
    },
  };
}

function getMockAnswerContainingEntity() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('x', getMockEntity1());
      return map;
    },
  };
}

function getMockAnswerContainingTypeInstances() {
  return {
    explanation: () => {},
    map: () => {
      const map = new Map();
      map.set('x', getMockEntity1());
      return map;
    },
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
  getMockAnswer1,
  getMockQueryPattern1,
  getMockAnswer2,
  getMockQueryPattern2,
  getMockAnswer3,
  getMockQueryPattern3,
  getMockAnswerContainingImplicitType,
  getMockAnswerContainingRelationship,
  getMockAnswerContainingEntity,
  getMockAnswerContainingTypeInstances,
};

