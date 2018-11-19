import {
  OPEN_GRAKN_TX,
  LOAD_SCHEMA,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
  UPDATE_METATYPE_INSTANCES,
  INITIALISE_VISUALISER,
  DEFINE_ENTITY_TYPE,
  COMMIT_TX,
  DEFINE_ROLE,
  DEFINE_ATTRIBUTE_TYPE,
  DEFINE_RELATIONSHIP_TYPE,
  DELETE_TYPE,
  DELETE_SCHEMA_CONCEPT,
  REFRESH_SELECTED_NODE,
  DELETE_PLAYS_ROLE,
  DELETE_RELATES_ROLE,
  DELETE_ATTRIBUTE,
  ADD_TYPE,
} from '@/components/shared/StoresActions';
import Grakn from 'grakn';
import SchemaHandler from '../SchemaHandler';
import {
  META_CONCEPTS,
  computeSubConcepts,
  relationshipTypesOutboundEdges,
  updateNodePositions,
  loadMetaTypeInstances,
  typeInboundEdges,
} from '../SchemaUtils';
import SchemaCanvasEventsHandler from '../SchemaCanvasEventsHandler';

export default {

  [OPEN_GRAKN_TX]({ state, commit }) {
    const graknTx = state.graknSession.transaction(Grakn.txType.WRITE);
    commit('setSchemaHandler', new SchemaHandler(graknTx));
    return graknTx;
  },

  async [CURRENT_KEYSPACE_CHANGED]({ state, dispatch, commit, rootState }, keyspace) {
    if (keyspace !== state.currentKeyspace) {
      dispatch(CANVAS_RESET);
      commit('currentKeyspace', keyspace);
      commit('graknSession', rootState.grakn.session(keyspace));
      await dispatch(UPDATE_METATYPE_INSTANCES);
      dispatch(LOAD_SCHEMA);
    }
  },

  [CANVAS_RESET]({ state, commit }) {
    state.visFacade.resetCanvas();
    commit('selectedNodes', null);
    commit('updateCanvasData');
  },

  [INITIALISE_VISUALISER]({ state, commit, dispatch }, { container, visFacade }) {
    commit('setVisFacade', visFacade.initVisualiser(container, state.visStyle));
    SchemaCanvasEventsHandler.registerHandlers({ state, commit, dispatch });
  },

  async [UPDATE_METATYPE_INSTANCES]({ dispatch, commit }) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const metaTypeInstances = await loadMetaTypeInstances(graknTx);
    graknTx.close();
    commit('metaTypeInstances', metaTypeInstances);
  },

  async [LOAD_SCHEMA]({ state, commit, dispatch }) {
    if (!state.visFacade) return;
    commit('loadingSchema', true);

    const graknTx = await dispatch(OPEN_GRAKN_TX);

    const response = (await (await graknTx.query('match $x sub thing; get;')).collect());

    const concepts = response.map(answer => Array.from(answer.map().values())).flatMap(x => x);
    const explicitConcepts = await Promise.all(concepts
      .map(async type => ((!await type.isImplicit()) ? type : null)))
      .then(explicits => explicits.filter(l => l));

    const labelledNodes = await Promise.all(explicitConcepts.map(async x => Object.assign(x, { label: await x.label() })));

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

    graknTx.close();

    nodes = updateNodePositions(nodes);
    state.visFacade.addToCanvas({ nodes, edges: relEdges.concat(subConcepts.edges) });
    state.visFacade.fitGraphToWindow();
    commit('loadingSchema', false);
  },

  async [COMMIT_TX](graknTx) {
    return graknTx.commit();
  },

  async [DEFINE_ENTITY_TYPE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.defineEntityType(payload);

    dispatch(COMMIT_TX, graknTx).then(async () => {
      const type = await graknTx.getSchemaConcept(payload.label);
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
      state.visFacade.addToCanvas({ nodes, edges });
    })
      .catch((e) => { throw e; });
  },

  async [DEFINE_ROLE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.defineRole(payload);
    dispatch(COMMIT_TX, graknTx)
      .catch((e) => { throw e; });
  },

  async [DEFINE_ATTRIBUTE_TYPE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.defineAttributeType(payload);
    dispatch(COMMIT_TX, graknTx)
      .catch((e) => { throw e; });
  },

  async [DEFINE_RELATIONSHIP_TYPE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.defineRelationshipType(payload);

    dispatch(COMMIT_TX, graknTx).then(async () => {
      const type = await graknTx.getSchemaConcept(payload.label);
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
      state.visFacade.addToCanvas({ nodes, edges });
    })
      .catch((e) => { throw e; });
  },

  async [DELETE_TYPE]({ state, dispatch, commit }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const typeId = await state.schemaHandler.deleteType(payload);
    dispatch(COMMIT_TX, graknTx).then(() => {
      state.visFacade.deleteFromCanvas([typeId]);
      commit('selectedNodes'.null);
    })
      .catch((e) => { throw e; });
  },

  // Difference with action above: this deletes a schema concept that is not shown in canvas
  async [DELETE_SCHEMA_CONCEPT]({ state, dispatch, commit }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.defineRelationshipType(payload);
    dispatch(COMMIT_TX, graknTx).then(() => {
      commit('selectedNodes'.null);
    })
      .catch((e) => { throw e; });
  },

  async [REFRESH_SELECTED_NODE]({ state, commit }) {
    const node = state.selectedNodes[0];
    if (!node) return;
    commit('selectedNodes', null);
    commit('selectedNodes', [node.id]);
  },

  async [DELETE_PLAYS_ROLE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.deletePlaysRole(payload);
    dispatch(COMMIT_TX, graknTx).then(async () => {
      const type = await graknTx.getSchemaConcept(payload.label);
      state.visFacade.deleteEdgesOnNode(type.id, payload.roleName);
      dispatch(REFRESH_SELECTED_NODE);
    })
      .catch((e) => { throw e; });
  },

  async [DELETE_RELATES_ROLE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.deleteRelatesRole(payload);
    dispatch(COMMIT_TX, graknTx).then(async () => {
      const type = await graknTx.getSchemaConcept(payload.label);
      state.visFacade.deleteEdgesOnNode(type.id, payload.roleName);
      dispatch(REFRESH_SELECTED_NODE);
    })
      .catch((e) => { throw e; });
  },

  async [DELETE_ATTRIBUTE]({ dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    await this.schemaHandler.deleteAttribute(payload);
    dispatch(COMMIT_TX, graknTx).then(() => {
      dispatch(REFRESH_SELECTED_NODE);
    })
      .catch((e) => { throw e; });
  },

  async [ADD_TYPE]({ dispatch, state }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    switch (payload.type) {
      case 'attribute': {
        await this.schemaHandler.addAttribute({ label: state.selectedNodes[0].label, typeLabel: payload.typeLabel });
        break;
      }
      case 'plays': {
        await this.schemaHandler.addPlaysRole({ label: state.selectedNodes[0].label, typeLabel: payload.typeLabel });
        const type = await graknTx.getSchemaConcept(state.selectedNodes[0].label);
        const relatesEdges = await relationshipTypesOutboundEdges([type]);
        state.visFacade.addToCanvas({ nodes: [], edges: relatesEdges });
        break;
      }
      case 'relates': {
        await this.schemaHandler.addRelatesRole({ label: state.selectedNodes[0].label, typeLabel: payload.typeLabel });
        const type = await graknTx.getSchemaConcept(state.selectedNodes[0].label);
        const relatesEdges = await relationshipTypesOutboundEdges([type]);
        state.visFacade.addToCanvas({ nodes: [], edges: relatesEdges });
        break;
      }
      default:
        // do nothing
    }
    dispatch(REFRESH_SELECTED_NODE);
  },
};
