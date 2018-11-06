import Style from './Style';
import NodeSettings from './RightBar/SettingsTab/DisplaySettings';
import QuerySettings from './RightBar/SettingsTab/QuerySettings';

// Map graql variables and explanations to each concept
async function attachExplanation(result) {
  return result.map((x) => {
    const exp = x.explanation();
    const key = x.map().keys().next().value;

    return Array.from(x.map().values()).flatMap((y) => {
      y.explanation = exp;
      y.graqlVar = key;
      return y;
    });
  }).flatMap(x => x);
}

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
      label = `${node.type}: ${await node.value}`;
      break;
    case 'RELATIONSHIP':
      label = '';
      break;
    default:
      label = node.type;
  }
  return label;
}

async function prepareSchemaConcept(schemaConcept) {
  schemaConcept.label = await schemaConcept.label();
  // schemaConcept.attributes = await computeAttributes(schemaConcept);
}

async function prepareEntity(entity) {
  entity.type = await (await entity.type()).label();
  entity.label = await buildLabel(entity);
  entity.isInferred = await entity.isInferred();
}

async function prepareRelationship(rel) {
  rel.type = await (await rel.type()).label();
  rel.isInferred = await rel.isInferred();
}

async function prepareAttribute(attribute) {
  attribute.type = await (await attribute.type()).label();
  attribute.value = await attribute.value();
  attribute.label = await buildLabel(attribute);
  attribute.isInferred = await attribute.isInferred();
}

/**
 * For each node contained in concepts param, based on baseType, fetch type, value
 * and build label
 * @param {Object[]} concepts array of gRPC concepts to prepare
 */
async function prepareNodes(concepts) {
  const nodes = [];

  await Promise.all(concepts.map(async (concept) => {
    switch (concept.baseType) {
      case 'ENTITY_TYPE':
      case 'ATTRIBUTE_TYPE':
      case 'RELATIONSHIP_TYPE':
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
  }));

  return nodes;
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
      switch (thing.baseType) {
        case 'ENTITY':
          await prepareEntity(thing);
          break;
        case 'ATTRIBUTE':
          await prepareAttribute(thing);
          break;
        case 'RELATIONSHIP':
          await prepareRelationship(thing);
          break;
        default:
          throw new Error(`Unrecognised baseType of thing: ${thing.baseType}`);
      }
      thing.offset = 0;
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

async function computeAttributeEdges(attributes, thingIds) {
  return Promise.all(attributes.map(async (attr) => {
    const owners = await (await attr.owners()).collect();
    const ownersInMap = owners.filter(owner => thingIds.includes(owner.id));
    return ownersInMap.map(owner => ({ from: owner.id, to: attr.id, label: 'has' }),
    );
  }));
}

async function constructEdges(result) {
  const conceptMaps = result.map(x => Array.from(x.map().values()));

  // Edges are a combination of relationship edges and attribute edges
  const edges = await Promise.all(conceptMaps.map(async (map) => {
    // collect ids of all entities in a concept map
    const thingIds = map.map(x => x.id);

    const attributes = map.filter(x => x.isAttribute());
    const relationships = map.filter(x => x.isRelationship());

    // Compute edges that connect things to their attributes
    const attributeEdges = await computeAttributeEdges(attributes, thingIds);

    const roleplayers = await relationshipsRolePlayers(relationships, false);
    // Compute edges that connect things to their role players
    const relationshipEdges = roleplayers.edges.filter(edge => thingIds.includes(edge.to));
    // Combine attribute and relationship edges
    return attributeEdges.concat(relationshipEdges).flatMap(x => x);
  }));
  return edges.flatMap(x => x);
}

async function buildFromConceptMap(result, autoLoadRolePlayers, limitRoleplayers) {
  const nodes = await prepareNodes(await attachExplanation(result));
  const edges = await constructEdges(result);

  // Check if auto-load role players is selected
  if (autoLoadRolePlayers) {
    const relationships = nodes.filter(x => x.baseType === 'RELATIONSHIP');
    const roleplayers = await relationshipsRolePlayers(relationships, limitRoleplayers, QuerySettings.getNeighboursLimit());

    nodes.push(...roleplayers.nodes);
    edges.push(...roleplayers.edges);
  }
  return { nodes, edges };
}

async function buildFromConceptList(path, pathNodes) {
  const data = { nodes: await prepareNodes(pathNodes), edges: [] };

  const relationships = data.nodes.filter(x => x.baseType === 'RELATIONSHIP');

  const roleplayers = await relationshipsRolePlayers(relationships);

  roleplayers.nodes.filter(x => path.list().includes(x.id));
  roleplayers.edges.filter(x => (path.list().includes(x.to) && path.list().includes(x.to)));

  data.nodes.push(...roleplayers.nodes);
  data.edges.push(...roleplayers.edges);

  data.edges.map(edge => Object.assign(edge, Style.computeShortestPathEdgeStyle()));
  return data;
}


export default {
  buildFromConceptMap,
  buildFromConceptList,
  prepareNodes,
  relationshipsRolePlayers,
};
