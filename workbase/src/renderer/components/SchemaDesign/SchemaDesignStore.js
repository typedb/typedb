import storage from '@/components/shared/PersistentStorage';
import Vue from 'vue';
import SchemaHandler from './SchemaHandler';
import CanvasStoreMixin from '../shared/CanvasStoreMixin';
import Style from './Style';

import { DEFINE_ENTITY_TYPE, DEFINE_ATTRIBUTE_TYPE, DEFINE_RELATIONSHIP_TYPE,
  DELETE_TYPE, LOAD_SCHEMA, DEFINE_ROLE, DELETE_SCHEMA_CONCEPT, ADD_TYPE } from '../shared/StoresActions';


const relationshipTypesOutboundEdges = async (nodes) => {
  const edges = [];
  const promises = nodes.filter(x => x.isRelationshipType())
    .map(async rel =>
      Promise.all((await rel.relates()).map(async (role) => {
        const types = await role.playedByTypes();
        const label = await role.getLabel();
        return types.forEach((type) => { edges.push({ from: rel.id, to: type.id, label }); });
      })),
    );
  await Promise.all(promises);
  return edges;
};

const META_CONCEPTS = new Set(['entity', 'relationship', 'attribute', 'role']);

const computeSubConcepts = async (nodes) => {
  const edges = [];
  const subConcepts = [];
  await Promise.all(nodes.map(async (concept) => {
    const sup = await concept.sup();
    if (sup) {
      const supLabel = await sup.getLabel();
      if (!META_CONCEPTS.has(supLabel)) {
        edges.push({ from: concept.id, to: sup.id, label: 'isa' });
        subConcepts.push(concept);
      }
    }
  }));
  return { nodes: subConcepts, edges };
};

const typeInboundEdges = async (type, visFacade) => {
  const roles = await type.plays();
  const relationshipTypes = await Promise.all(roles.map(role => role.relationshipTypes())).then(rels => rels.flatMap(x => x));
  return relationshipTypesOutboundEdges(relationshipTypes.filter(rel => visFacade.getNode(rel)));
};


const actions = {
  [DEFINE_ENTITY_TYPE](payload) {
    return this.schemaHandler.defineEntityType(payload)
      .then(this.commitAndRefreshTx)
      .then(async () => {
        const type = await this.graknTx.getSchemaConcept(payload.label);
        const sup = await type.sup();
        const supLabel = await sup.getLabel();
        let edges;
        // If the supertype is a concept defined by user
        // we just draw the isa edge instead of all edges from relationshipTypes
        if (!META_CONCEPTS.has(supLabel)) {
          edges = [{ from: type.id, to: sup.id, label: 'isa' }];
        } else {
          edges = await typeInboundEdges(type, this.visFacade);
        }
        const label = await type.getLabel();
        const nodes = [Object.assign(type, { label })];
        this.visFacade.addToCanvas({ nodes, edges });
      })
      .catch((e) => { this.openGraknTx(); throw e; });
  },
  [DEFINE_ROLE](payload) {
    return this.schemaHandler.defineRole(payload)
      .then(this.commitAndRefreshTx)
      .catch((e) => { this.openGraknTx(); throw e; });
  },
  [DEFINE_ATTRIBUTE_TYPE](payload) {
    return this.schemaHandler.defineAttributeType(payload)
      .then(this.commitAndRefreshTx)
      .catch((e) => { this.openGraknTx(); throw e; });
  },
  [DEFINE_RELATIONSHIP_TYPE](payload) {
    return this.schemaHandler.defineRelationshipType(payload)
      .then(this.commitAndRefreshTx)
      .then(async () => {
        const type = await this.graknTx.getSchemaConcept(payload.label);
        const label = await type.getLabel();
        const sup = await type.sup();
        const supLabel = await sup.getLabel();
        let edges;
        // If the supertype is a concept defined by user
        // we just draw the isa edge instead of all edges to roleplayers
        if (!META_CONCEPTS.has(supLabel)) {
          edges = [{ from: type.id, to: sup.id, label: 'isa' }];
        } else {
          const relatesEdges = await relationshipTypesOutboundEdges([type]);
          const plays = await typeInboundEdges(type, this.visFacade);
          edges = plays.concat(relatesEdges);
        }
        const nodes = [Object.assign(type, { label })];
        this.visFacade.addToCanvas({ nodes, edges });
      }).catch((e) => { this.openGraknTx(); throw e; });
  },
  [DELETE_TYPE](payload) {
    return this.schemaHandler.deleteType(payload)
      .then((typeId) => { this.visFacade.deleteFromCanvas([typeId]); })
      .then(this.commitAndRefreshTx)
      .then(() => {
        this.setSelectedNode(null);
      })
      .catch((e) => { this.openGraknTx(); throw e; });
  },
  [DELETE_SCHEMA_CONCEPT](payload) { // Difference with method above: this deletes a schema concept that is not shown in canvas
    return this.schemaHandler.deleteType(payload)
      .then(this.commitAndRefreshTx)
      .then(() => {
        this.setSelectedNode(null);
      }).catch((e) => { this.openGraknTx(); throw e; });
  },
  async [LOAD_SCHEMA]() {
    if (!this.visFacade) return;
    this.loadingSchema = true;
    const response = await this.graknTx.execute('match $x sub thing; get;');
    const concepts = response.map(map => Array.from(map.values())).flatMap(x => x);
    const explicitConcepts = await Promise.all(concepts
      .map(async type => ((!await type.isImplicit()) ? type : null)))
      .then(explicits => explicits.filter(l => l));
    const labelledNodes = await Promise.all(explicitConcepts.map(async x => Object.assign(x, { label: await x.getLabel() })));
    let nodes = labelledNodes
      .filter(x => !x.isAttributeType())
      .filter(x => x.label !== 'thing')
      .filter(x => x.label !== 'entity')
      .filter(x => x.label !== 'attribute')
      .filter(x => x.label !== 'relationship');

    // Find nodes that are subconcepts of existing types - these nodes will only have isa edges
    const subConcepts = await computeSubConcepts(nodes);
    const subConceptsIds = new Set(subConcepts.nodes.map(n => n.id));
    const relNodes = nodes.filter(x => !subConceptsIds.has(x.id));
    // Draw all edges from relationships to roleplayers only on concepts that don't subtype a custom type
    const relEdges = await relationshipTypesOutboundEdges(relNodes);

    nodes = this.updateNodePositions(nodes);

    this.visFacade.addToCanvas({ nodes, edges: relEdges.concat(subConcepts.edges) });
    this.visFacade.fitGraphToWindow();
    this.loadingSchema = false;
  },
  [ADD_TYPE](payload) {
    switch (payload.type) {
      case 'attribute':
        return this.addAttribute(payload);
      case 'plays':
        return this.addPlaysRole(payload);
      case 'relates':
        return this.addRelatesRole(payload);
      default:
        return null;
    }
  },
  deletePlaysRole(payload) {
    return this.schemaHandler.deletePlaysRole(payload).then(async () => {
      const type = await this.graknTx.getSchemaConcept(payload.label);
      this.visFacade.deleteEdgesOnNode(type.id, payload.roleName);
      this.refreshSelectedNode();
    }).catch((e) => { this.openGraknTx(); throw e; });
  },
  deleteRelatesRole(payload) {
    return this.schemaHandler.deleteRelatesRole(payload)
      .then(this.commitAndRefreshTx)
      .then(async () => {
        const type = await this.graknTx.getSchemaConcept(payload.label);
        this.visFacade.deleteEdgesOnNode(type.id, payload.roleName);
        this.refreshSelectedNode();
      }).catch((e) => { this.openGraknTx(); throw e; });
  },
  deleteAttribute(payload) {
    return this.schemaHandler.deleteAttribute(payload)
      .then(this.commitAndRefreshTx)
      .then(() => {
        this.refreshSelectedNode();
      }).catch((e) => { this.openGraknTx(); throw e; });
  },
};

const watch = {
  graknTx(tx) {
    // TODO: find a way to Load schema only if it's the first time we create the graknTx
    // Don't reload schema if reopening a tx because previous one crashed or other resons.
    // if (!oldTx) {
    this.dispatch(LOAD_SCHEMA);
    this.schemaHandler = new SchemaHandler(tx);
    // }
  },
  isInit() {
    this.registerCanvasEventHandler('dragEnd', (params) => {
      if (!params.nodes.length) return;
      let positionMap = storage.get('schema-node-positions');

      if (positionMap) {
        positionMap = JSON.parse(positionMap);
      } else {
        positionMap = {};
        storage.set('schema-node-positions', {});
      }
      params.nodes.forEach((nodeId) => {
        positionMap[nodeId] = params.pointer.canvas;
      });
      storage.set('schema-node-positions', JSON.stringify(positionMap));
    });
  },
};


const methods = {
  getVisStyle() { return Style; }, // TODO implement vis style for schema design
  refreshSelectedNode() {
    const node = this.getSelectedNode();
    if (!node) return;
    this.setSelectedNode(null);
    this.setSelectedNode(node.id);
  },
  commitAndRefreshTx() {
    return this.graknTx.commit().then(() => this.openGraknTx());
  },
  updateNodePositions(nodes) {
    let positionMap = storage.get('schema-node-positions');
    if (positionMap) {
      positionMap = JSON.parse(positionMap);
      nodes.forEach((node) => {
        if (node.id in positionMap) {
          const { x, y } = positionMap[node.id];
          Object.assign(node, { x, y, fixed: { x: true, y: true } });
        }
      });
    }
    return nodes;
  },
  addAttribute(payload) {
    return this.schemaHandler.addAttribute({ label: this.getSelectedNode().label, typeLabel: payload.typeLabel }).then(() => {
      this.refreshSelectedNode();
    }).catch((e) => { this.openGraknTx(); throw e; });
  },
  addRelatesRole(payload) {
    return this.schemaHandler.addRelatesRole({ label: this.getSelectedNode().label, typeLabel: payload.typeLabel }).then(async () => {
      const type = await this.graknTx.getSchemaConcept(this.getSelectedNode().label);
      const relatesEdges = await relationshipTypesOutboundEdges([type]);
      this.visFacade.addToCanvas({ nodes: [], edges: relatesEdges });
      this.refreshSelectedNode();
    }).catch((e) => { this.openGraknTx(); throw e; });
  },
  addPlaysRole(payload) {
    return this.schemaHandler.addPlaysRole({ label: this.getSelectedNode().label, typeLabel: payload.typeLabel }).then(async () => {
      const type = await this.graknTx.getSchemaConcept(this.getSelectedNode().label);
      const relatesEdges = (type.isRelationshipType()) ? await relationshipTypesOutboundEdges([type]) : [];
      const plays = await typeInboundEdges(type, this.visFacade);
      this.visFacade.addToCanvas({ nodes: [], edges: plays.concat(relatesEdges) });
      this.refreshSelectedNode();
    }).catch((e) => { this.openGraknTx(); throw e; });
  },
  showSpinner() {
    return this.loadingSchema;
  },
  getEditingMode() {
    return this.editingMode;
  },
  setEditingMode(mode) {
    this.editingMode = mode;
  },
};

const state = {
  schemaHandler: undefined,
  loadingSchema: false,
  editingMode: undefined,
};

export default new Vue({
  mixins: [CanvasStoreMixin.create()],
  data() { return Object.assign(state, { actions }); },
  methods,
  watch,
});

