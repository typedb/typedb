import QuerySettings from './RightBar/SettingsTab/QuerySettings';
import VisualiserGraphBuilder from './VisualiserGraphBuilder';
const LETTER_G_KEYCODE = 71;
const HAS_ATTRIBUTE_LABEL = 'has';
const ISA_INSTANCE_LABEL = 'isa';

function getNeighboursQuery(node, neighboursLimit) {
  switch (node.baseType) {
    case 'ENTITY_TYPE':
    case 'ATTRIBUTE_TYPE':
    case 'RELATIONSHIP_TYPE':
      return `match $x id "${node.id}"; $y isa $x; offset ${node.offset}; limit ${neighboursLimit}; get $y;`;
    case 'ENTITY':
      return `match $x id "${node.id}"; $r ($x, $y); offset ${node.offset}; limit ${neighboursLimit}; get $r, $y;`;
    case 'ATTRIBUTE':
      return `match $x has attribute $y; $y id "${node.id}"; offset ${node.offset}; limit ${neighboursLimit}; get $x;`;
    case 'RELATIONSHIP':
      return `match $r id "${node.id}"; $r ($x, $y); offset ${node.offset}; limit ${neighboursLimit}; get $x;`;
    default:
      throw new Error(`Unrecognised baseType of thing: ${node.baseType}`);
  }
}

export function limitQuery(query) {
  const getRegex = /^((.|\s)*;)\s*(get\b.*;)$/;
  let limitedQuery = query;

  // If there is no `get` the user mistyped the query
  if (getRegex.test(query)) {
    const limitRegex = /.*;\s*(limit\b.*?;).*/;
    const offsetRegex = /.*;\s*(offset\b.*?;).*/;
    const deleteRegex = /^(.*;)\s*(delete\b.*;)$/;
    const match = getRegex.exec(query);
    limitedQuery = match[1];
    const getPattern = match[3];
    if (!(offsetRegex.test(query)) && !(deleteRegex.test(query))) { limitedQuery = `${limitedQuery} offset 0;`; }
    if (!(limitRegex.test(query)) && !(deleteRegex.test(query))) { limitedQuery = `${limitedQuery} limit ${QuerySettings.getQueryLimit()};`; }
    limitedQuery = `${limitedQuery} ${getPattern}`;
  }
  return limitedQuery;
}


export function buildExplanationQuery(answer, queryPattern) {
  let query = 'match ';
  let attributeQuery = null;
  Array.from(answer.map().entries()).forEach(([graqlVar, concept]) => {
    if (concept.isAttribute()) {
      const attributeRegex = queryPattern.match(/(?:has )(\w+)/);
      if (attributeRegex) {
        attributeQuery = `has ${queryPattern.match(/(?:has )(\w+)/)[1]} $${graqlVar};`;
      } else {
        let attributeType = queryPattern.match(/\(([^)]+)\)/)[0].split(',').filter(y => y.includes(graqlVar));
        attributeType = attributeType[0].slice(1, attributeType[0].indexOf(':'));
        attributeQuery = `has ${attributeType} $${graqlVar};`;
      }
      // attributeQuery = `has ${queryPattern.match(/(?:has )(\w+)/)[1]} $${graqlVar};`;
    } else if (concept.isEntity()) {
      query += `$${graqlVar} id ${concept.id}; `;
    }
  });
  return { query, attributeQuery };
}

export function computeAttributes(nodes) {
  return Promise.all(nodes.map(async (node) => {
    const attributes = await (await node.attributes()).collect();
    node.attributes = await Promise.all(attributes.map(async (concept) => {
      const attribute = {};
      if (concept.isType()) {
        await concept.label().then((label) => { attribute.type = label; });
      } else {
        await Promise.all([
          concept.type().then(type => type.label()).then((label) => { attribute.type = label; }),
          concept.value().then((value) => { attribute.value = value; }),
        ]);
      }
      return attribute;
    }));
    return node;
  }));
}

export async function loadMetaTypeInstances(graknTx) {
// Fetch types
  const entities = await (await graknTx.query('match $x sub entity; get;')).collectConcepts();
  const rels = await (await graknTx.query('match $x sub relationship; get;')).collectConcepts();
  const attributes = await (await graknTx.query('match $x sub attribute; get;')).collectConcepts();
  const roles = await (await graknTx.query('match $x sub role; get;')).collectConcepts();

  // Get types labels
  const metaTypeInstances = {};
  metaTypeInstances.entities = await Promise.all(entities.map(type => type.label()))
    .then(labels => labels.filter(l => l !== 'entity')
      .concat()
      .sort());
  metaTypeInstances.relationships = await Promise.all(rels.map(async type => ((!await type.isImplicit()) ? type.label() : null)))
    .then(labels => labels.filter(l => l && l !== 'relationship')
      .concat()
      .sort());
  metaTypeInstances.attributes = await Promise.all(attributes.map(type => type.label()))
    .then(labels => labels.filter(l => l !== 'attribute')
      .concat()
      .sort());
  metaTypeInstances.roles = await Promise.all(roles.map(async type => ((!await type.isImplicit()) ? type.label() : null)))
    .then(labels => labels.filter(l => l && l !== 'role')
      .concat()
      .sort());
  return metaTypeInstances;
}

export function validateQuery(query) {
  const trimmed = query.trim();
  if (/^(.*;)\s*(delete\b.*;)$/.test(trimmed) || /^(.*;)\s*(delete\b.*;)$/.test(trimmed)
        || /^insert/.test(trimmed)
        || /^(.*;)\s*(aggregate\b.*;)$/.test(trimmed) || /^(.*;)\s*(aggregate\b.*;)$/.test(trimmed)
        || (/^compute/.test(trimmed) && !trimmed.startsWith('compute path'))) {
    throw new Error('Only get and compute path queries are supported for now.');
  }
}
export function addResetGraphListener(dispatch, action) {
  window.addEventListener('keydown', (e) => {
  // Reset canvas when metaKey(CtrlOrCmd) + G are pressed
    if ((e.keyCode === LETTER_G_KEYCODE) && e.metaKey) { dispatch(action); }
  });
}


/**
 * Given a Grakn Answer, this function returns the query that needs to be run in order
 * to obtain a visual explanation of the inferred concept
 * @param {Object} answer Grakn Answer which contains the explanation that needs to be loaded
 */
export function mapAnswerToExplanationQuery(answer) {
  const queryPattern = answer.explanation().queryPattern();
  let query = buildExplanationQuery(answer, queryPattern).query;
  if (queryPattern.includes('has')) {
    query += `${buildExplanationQuery(answer, queryPattern).attributeQuery} get;`;
  } else {
    query += `$r ${queryPattern.slice(1, -1).match(/\((.*?;)/)[0]} offset 0; limit 1; get $r;`;
  }
  return query;
}

/**
 * Checks if a ConceptMap inside the provided Answer contains at least one implicit concept
 * @param {Object} answer ConceptMap Answer to be inspected
 * @return {Boolean}
 */
async function answerContainsImplicitType(answer) {
  const concepts = Array.from(answer.map().values());
  return Promise.all(concepts.map(async concept => ((concept.isThing()) ? (await concept.type()).isImplicit() : concept.isImplicit())))
    .then(a => a.includes(true));
}

/**
 * Filters out Answers that contained inferred concepts in their ConceptMap
 * @param {Object[]} answers array of ConceptMap Answers to be inspected
 * @return {Object[]} filtered array of Answers
 */
export async function filterMaps(answers) { // Filter out ConceptMaps that contain implicit relationships
  return Promise.all(answers.map(async x => ((await answerContainsImplicitType(x)) ? null : x)))
    .then(maps => maps.filter(map => map));
}

/**
 * Executes query to load neighbours of given node and filters our all the answers that contain implicit concepts, given that we
 * don't want to show implicit concepts (relationships to attributes) to the user, for now.
 * @param {Object} node VisJs node of which we want to load the neighbours
 * @param {Object} graknTx Grakn transaction used to execute query
 * @param {Number} limit Limit of neighbours to load
 */
async function getFilteredNeighbourAnswers(node, graknTx, limit) {
  const query = getNeighboursQuery(node, limit);
  const resultAnswers = await (await graknTx.query(query)).collect();
  const filteredResult = await filterMaps(resultAnswers);
  if (resultAnswers.length !== filteredResult.length) {
    const offsetDiff = resultAnswers.length - filteredResult.length;
    node.offset += QuerySettings.getNeighboursLimit();
    return filteredResult.concat(await getFilteredNeighbourAnswers(node, graknTx, offsetDiff));
  }
  return resultAnswers;
}

async function getTypeNeighbours(node, graknTx, limit) {
  const filteredResult = await getFilteredNeighbourAnswers(node, graknTx, limit);
  const nodes = filteredResult.map(x => Array.from(x.map().values())).flatMap(x => x);
  const edges = nodes.map(instance => ({ from: instance.id, to: node.id, label: ISA_INSTANCE_LABEL }));
  return { nodes, edges };
}

async function getEntityNeighbours(node, graknTx, limit) {
  const filteredResult = await getFilteredNeighbourAnswers(node, graknTx, limit);
  const nodes = filteredResult.map(x => Array.from(x.map().values())).flatMap(x => x);
  const relationships = filteredResult.map(x => Array.from(x.map().values())).flatMap(x => x).filter(x => x.isRelationship());
  const roleplayers = await VisualiserGraphBuilder.relationshipsRolePlayers(relationships, false);
  const edges = roleplayers.edges.flatMap(x => x);
  return { nodes, edges };
}

async function getAttributeNeighbours(node, graknTx, limit) {
  const filteredResult = await getFilteredNeighbourAnswers(node, graknTx, limit);
  const nodes = filteredResult.map(x => Array.from(x.map().values())).flatMap(x => x);
  const edges = nodes.map(owner => ({ from: owner.id, to: node.id, label: HAS_ATTRIBUTE_LABEL }));
  return { nodes, edges };
}

async function getRelationshipNeighbours(node, graknTx, limit) {
  const filteredResult = await getFilteredNeighbourAnswers(node, graknTx, limit);
  const nodes = filteredResult.map(x => Array.from(x.map().values())).flatMap(x => x);
  const roleplayersIds = nodes.map(x => x.id);
  const relationshipConcept = await graknTx.getConcept(node.id);
  const roleplayers = Array.from((await relationshipConcept.rolePlayersMap()).entries());
  const edges = (await Promise.all(Array.from(roleplayers, async ([role, setOfThings]) => {
    const roleLabel = await role.label();
    return Array.from(setOfThings.values())
      .filter(thing => roleplayersIds.includes(thing.id))
      .map(thing => ({ from: node.id, to: thing.id, label: roleLabel }));
  }))).flatMap(x => x);
  return { nodes, edges };
}

export async function getNeighboursData(visNode, graknTx, neighboursLimit) {
  switch (visNode.baseType) {
    case 'ENTITY_TYPE':
    case 'ATTRIBUTE_TYPE':
    case 'RELATIONSHIP_TYPE':
      return getTypeNeighbours(visNode, graknTx, neighboursLimit);
    case 'ENTITY':
      return getEntityNeighbours(visNode, graknTx, neighboursLimit);
    case 'RELATIONSHIP':
      return getRelationshipNeighbours(visNode, graknTx, neighboursLimit);
    case 'ATTRIBUTE':
      return getAttributeNeighbours(visNode, graknTx, neighboursLimit);
    default:
      throw new Error(`Unrecognised baseType of thing: ${visNode.baseType}`);
  }
}
