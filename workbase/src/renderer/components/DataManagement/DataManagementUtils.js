import QuerySettings from './DataManagementContent/MenuBar/QuerySettings/QuerySettings';

function loadNeighbours(node, neighboursLimit) {
  let query;
  switch (node.baseType) {
    case 'ENTITY_TYPE':
    case 'ATTRIBUTE_TYPE':
    case 'RELATIONSHIP_TYPE':
    case 'ENTITY':
      query = `match $x id "${node.id}"; $y isa entity; $r ($x, $y); offset ${node.offset}; limit ${neighboursLimit}; get;`;
      break;
    case 'ATTRIBUTE':
      query = `match $x id "${node.id}"; $y isa entity; $r ($x, $y); offset ${node.offset}; limit ${neighboursLimit}; get;`;
      break;
    case 'RELATIONSHIP':
      query = `match $r id "${node.id}"; $r ($x, $y); offset ${node.offset}; limit ${neighboursLimit}; get $r, $x;`;
      break;
    default:
      throw new Error(`Unrecognised baseType of thing: ${node.baseType}`);
  }
  return query;
}

function limitQuery(query) {
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


function buildExplanationQuery(answer, queryPattern) {
  let query = 'match ';
  let attributeQuery = null;
  Array.from(answer.map().entries()).forEach(([graqlVar, concept]) => {
    if (concept.isAttribute()) {
      attributeQuery = `has ${queryPattern.match(/(?:has )(\w+)/)[1]} $${graqlVar};`;
    } else if (concept.isEntity()) {
      query += `$${graqlVar} id ${concept.id}; `;
    }
  });
  return { query, attributeQuery };
}


export default {
  loadNeighbours,
  limitQuery,
  buildExplanationQuery,
};
