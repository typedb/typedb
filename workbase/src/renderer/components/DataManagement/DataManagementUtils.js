import QuerySettings from './DataManagementContent/MenuBar/QuerySettings/QuerySettings';
import NodeSettings from './DataManagementContent/NodeSettingsPanel/NodeSettings';

function buildValue(array) {
  if (!array) return '';
  return array.join(', ');
}

async function labelFromStorage(node, attributeTypes) {
  const map = {};
  // Populate map with map[attributeType] = array of values (can be undefined)
  // This is because we need to handle 2 cases:
  // - when the current node does not have any attribute of type `attributeType`
  // - when the current node has multiple attributes of type 'attributeType'
  const promises = (await (await node.attributes()).collect()).map(async (attr) => {
    const label = await (await attr.type()).label();
    const value = await attr.value();
    if (!map[label]) map[label] = [value];
    else map[label].push(value);
  });
  await Promise.all(promises);
  const label = attributeTypes.map(type => `${type}: ${buildValue(map[type])}`).join('\n');
  // Always show node type when displaying node attributes on label
  return `${node.type}\n${label}`;
}


async function buildLabel(node) {
  const labels = NodeSettings.getTypeLabels(node.type);
  if (labels.length) return labelFromStorage(node, labels); // this function is async
  let label;
  switch (node.baseType) {
    case 'ENTITY':
      label = `${node.type}: ${node.id}`;
      break;
    case 'ATTRIBUTE':
      label = `${node.type}: ${node.value}`;
      break;
    case 'RELATIONSHIP':
      label = '';
      break;
    default:
      label = node.type;
  }
  return label;
}

async function loadRolePlayers(relationship, limitRolePlayers, limit, offset) {
  const nodes = [];
  const edges = [];

  let roleplayers = await relationship.rolePlayersMap();
  roleplayers = Array.from(roleplayers.entries());
  if (limitRolePlayers) {
    roleplayers = roleplayers.slice(offset, limit + offset);
  }

  // Build array of promises
  const promises = Array.from(roleplayers, async ([role, setOfThings]) => {
    const roleLabel = await role.label();
    await Promise.all(Array.from(setOfThings.values()).map(async (thing) => {
      const type = await thing.type();
      thing.type = await type.label();
      thing.label = await buildLabel(thing);
      thing.attrOffset = 0;
      nodes.push(thing);
      edges.push({ from: relationship.id, to: thing.id, label: roleLabel });
    }));
  });
  return Promise.all(promises).then((() => ({ nodes, edges })));
}

async function relationshipsRolePlayers(relationships, limitRolePlayers, limit) {
  const results = await Promise.all(relationships.map(rel => loadRolePlayers(rel, limitRolePlayers, limit, rel.offset)));
  return {
    nodes: results.flatMap(x => x.nodes),
    edges: results.flatMap(x => x.edges),
  };
}

async function prepareSchemaConcept(schemaConcept) {
  schemaConcept.label = await schemaConcept.label();
}

async function prepareEntity(entity) {
  entity.type = await (await entity.type()).label();
  entity.label = await buildLabel(entity);
}

async function prepareRelationship(rel) {
  rel.type = await (await rel.type()).label();
}

async function prepareAttribute(attribute) {
  attribute.type = await (await attribute.type()).label();
  attribute.value = await attribute.value();
  attribute.label = await buildLabel(attribute);
}

async function prepareConcepts(result) {
  debugger;
  return result.map((x) => {
    const exp = x.explanation();
    const key = x.map().keys().next().value;
    x = Array.from(x.map().values()).flatMap((x) => {
      x.explanation = exp;
      x.graqlVar = key;
      return x;
    });
    return x;
  }).reduce(
    (accumulator, currentValue) => accumulator.concat(currentValue),
    [],
  );
}

/**
 * For each node contained in concepts param, based on baseType, fetch type, value
 * and build label
 * @param {Object[]} concepts array of gRPC concepts to prepare
 */
async function prepareNodes(concepts) {
  const nodes = [];

  const promises = concepts.map(async (concept) => {
    switch (concept.baseType) {
      case 'ENTITY_TYPE':
      case 'ATTRIBUTE_TYPE':
      case 'RELATIONSHIP_TYPE':
        if (await concept.isImplicit()) return;
        await prepareSchemaConcept(concept);
        break;
      case 'ENTITY':
        await prepareEntity(concept);
        break;
      case 'ATTRIBUTE':
        await prepareAttribute(concept);
        break;
      case 'RELATIONSHIP': {
        await prepareRelationship(concept);
        break;
      }
      default:
        break;
    }
    concept.offset = 0;
    concept.attrOffset = 0;
    nodes.push(concept);
  });
  await Promise.all(promises);
  return nodes;
}


/* Methods handling double click */

async function consumeIterator(iterator, limit, offset) {
  const concepts = [];
  let item = await iterator.next();
  let i = 0;
  while (item !== null && i < (limit + offset)) {
    concepts.push(item);
    item = await iterator.next(); // eslint-disable-line no-await-in-loop
    i += 1;
  }
  return concepts;
}

async function showRelationshipsOfEntity(node, limit, offset) {
  const relationships = await consumeIterator(await node.relationships(), limit, offset);

  const filteredRelationships = [];

  await Promise.all(relationships.map(async (rel) => {
    const type = await rel.type();
    const isImplicit = await type.isImplicit();
    if (!isImplicit) {
      filteredRelationships.push(rel);
    }
  }));

  const nodes = await prepareNodes(filteredRelationships);

  const roleplayers = await relationshipsRolePlayers(filteredRelationships, false);
  const data = { nodes: nodes.concat(roleplayers.nodes), edges: roleplayers.edges };
  return data;
}

async function showAttributeOwners(node, limit, offset) {
  const owners = await consumeIterator(await node.owners(), limit, offset);

  const nodes = await prepareNodes(owners);
  const edges = owners.map(owner => ({ from: owner.id, to: node.id, label: 'has' }));
  return { nodes, edges };
}

async function showTypeInstances(node, limit, offset) {
  const instances = await consumeIterator(await node.instances(), limit, offset);

  const nodes = await prepareNodes(instances);
  const edges = instances.map(instance => ({ from: instance.id, to: node.id, label: 'isa' }));
  return { nodes, edges };
}

function loadNeighbours(node, neighboursLimit, offset) {
  switch (node.baseType) {
    case 'ENTITY_TYPE':
    case 'ATTRIBUTE_TYPE':
    case 'RELATIONSHIP_TYPE':
      return showTypeInstances(node, neighboursLimit, offset);
    case 'ENTITY':
      return showRelationshipsOfEntity(node, neighboursLimit, offset);
    case 'ATTRIBUTE':
      return showAttributeOwners(node, neighboursLimit, offset);
    case 'RELATIONSHIP':
      return loadRolePlayers(node, true, neighboursLimit, offset);
    default:
      return Promise.reject(new Error('BaseType not recognised.'));
  }
}

async function loadAttributes(node, limit, offset) {
  const attributes = await consumeIterator(await node.attributes(), limit, offset);
  const nodes = await prepareNodes(attributes);
  const edges = attributes.map(attribtue => ({ from: node.id, to: attribtue.id, label: 'has' }));
  return { nodes, edges };
}

function limitQuery(query) {
  const getRegex = /^(.*;)\s*(get\b.*;)$/;
  let limitedQuery = query;

  // If there is no `get` the user mistyped the query
  if (getRegex.test(query)) {
    const limitRegex = /.*;\s*(limit\b.*?;).*/;
    const offsetRegex = /.*;\s*(offset\b.*?;).*/;
    const deleteRegex = /^(.*;)\s*(delete\b.*;)$/;
    const match = getRegex.exec(query);
    limitedQuery = match[1];
    const getPattern = match[2];
    if (!(offsetRegex.test(query)) && !(deleteRegex.test(query))) { limitedQuery = `${limitedQuery} offset 0;`; }
    if (!(limitRegex.test(query)) && !(deleteRegex.test(query))) { limitedQuery = `${limitedQuery} limit ${QuerySettings.getQueryLimit()};`; }
    limitedQuery = `${limitedQuery} ${getPattern}`;
  }

  return limitedQuery;
}

function isRolePlayerAutoloadEnabled() {
  return QuerySettings.getRolePlayersStatus();
}

export default {
  prepareConcepts,
  prepareNodes,
  loadNeighbours,
  relationshipsRolePlayers,
  isRolePlayerAutoloadEnabled,
  limitQuery,
  loadAttributes,
};
