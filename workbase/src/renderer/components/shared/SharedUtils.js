export const META_CONCEPTS = new Set(['entity', 'relationship', 'attribute', 'role']);

export async function ownerHasEdges(nodes) {
  const edges = [];

  await Promise.all(nodes.map(async (node) => {
    const sup = await node.sup();
    if (sup) {
      const supLabel = await sup.label();
      if (META_CONCEPTS.has(supLabel)) {
        let attributes = await node.attributes();
        attributes = await attributes.collect();
        attributes.map(attr => edges.push({ from: node.id, to: attr.id, label: 'has' }));
      } else { // if node has a super type which is not a META_CONCEPT construct edges to attributes expect those which are inherited from its super type
        const supAttributeIds = (await (await sup.attributes()).collect()).map(x => x.id);

        const attributes = (await (await node.attributes()).collect()).filter(attr => !supAttributeIds.includes(attr.id));
        attributes.map(attr => edges.push({ from: node.id, to: attr.id, label: 'has' }));
      }
    }
  }));
  return edges;
}

export async function relationshipTypesOutboundEdges(nodes) {
  const edges = [];
  const promises = nodes.filter(x => x.isRelationshipType())
    .map(async rel =>
      Promise.all(((await (await rel.roles()).collect())).map(async (role) => {
        const types = await (await role.players()).collect();
        const label = await role.label();
        return types.forEach((type) => { edges.push({ from: rel.id, to: type.id, label }); });
      })),
    );
  await Promise.all(promises);
  return edges;
}

export async function computeSubConcepts(nodes) {
  const edges = [];
  const subConcepts = [];
  await Promise.all(nodes.map(async (concept) => {
    const sup = await concept.sup();
    if (sup) {
      const supLabel = await sup.label();
      if (!META_CONCEPTS.has(supLabel)) {
        edges.push({ from: concept.id, to: sup.id, label: 'sub' });
        subConcepts.push(concept);
      }
    }
  }));
  return { nodes: subConcepts, edges };
}
