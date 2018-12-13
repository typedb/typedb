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
  DELETE_SCHEMA_CONCEPT,
  REFRESH_SELECTED_NODE,
  DELETE_PLAYS_ROLE,
  DELETE_RELATES_ROLE,
  DELETE_ATTRIBUTE,
  ADD_TYPE,
} from '@/components/shared/StoresActions';
import logger from '@/../Logger';

import Grakn from 'grakn';
import SchemaHandler from '../SchemaHandler';
import {
  META_CONCEPTS,
  computeSubConcepts,
  relationshipTypesOutboundEdges,
  updateNodePositions,
  loadMetaTypeInstances,
  typeInboundEdges,
  computeAttributes,
} from '../SchemaUtils';
import SchemaCanvasEventsHandler from '../SchemaCanvasEventsHandler';

export default {
  async [OPEN_GRAKN_TX]({ state, commit }) {
    const graknTx = await state.graknSession.transaction(Grakn.txType.WRITE);
    commit('setSchemaHandler', new SchemaHandler(graknTx));
    return graknTx;
  },

  [CURRENT_KEYSPACE_CHANGED]({ state, dispatch, commit, rootState }, keyspace) {
    if (keyspace !== state.currentKeyspace) {
      dispatch(CANVAS_RESET);
      commit('currentKeyspace', keyspace);
      commit('graknSession', rootState.grakn.session(keyspace));
      dispatch(UPDATE_METATYPE_INSTANCES);
      dispatch(LOAD_SCHEMA);
    }
  },

  async [UPDATE_METATYPE_INSTANCES]({ dispatch, commit }) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const metaTypeInstances = await loadMetaTypeInstances(graknTx);
    graknTx.close();
    commit('metaTypeInstances', metaTypeInstances);
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


  async [LOAD_SCHEMA]({ state, commit, dispatch }) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    try {
      if (!state.visFacade) return;
      commit('loadingSchema', true);

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

      // Draw all edges from relationships to roleplayers
      const relEdges = await relationshipTypesOutboundEdges(nodes);

      nodes = updateNodePositions(nodes);
      state.visFacade.addToCanvas({ nodes, edges: relEdges.concat(subConcepts.edges) });
      state.visFacade.fitGraphToWindow();

      nodes = await computeAttributes(nodes);
      state.visFacade.updateNode(nodes);

      graknTx.close();
      commit('loadingSchema', false);
    } catch (e) {
      logger.error(e.stack);
      graknTx.close();
      commit('loadingSchema', false);
      throw e;
    }
  },

  async [COMMIT_TX](store, graknTx) {
    return graknTx.commit();
  },

  async [DEFINE_ENTITY_TYPE]({ state, dispatch }, payload) {
    let graknTx = await dispatch(OPEN_GRAKN_TX);

    // define entity type
    await state.schemaHandler.defineEntityType(payload);

    // add attribute types to entity type
    await Promise.all(payload.attributeTypes.map(async (attributeType) => {
      await state.schemaHandler.addAttribute({ schemaLabel: payload.entityLabel, attributeLabel: attributeType });
    }));

    // add roles to entity type
    await Promise.all(payload.roleTypes.map(async (roleType) => {
      await state.schemaHandler.addPlaysRole({ schemaLabel: payload.entityLabel, roleLabel: roleType });
    }));

    dispatch(COMMIT_TX, graknTx).then(async () => {
      graknTx = await dispatch(OPEN_GRAKN_TX);

      const type = await graknTx.getSchemaConcept(payload.entityLabel);
      const sup = await type.sup();
      const supLabel = await sup.label();
      let edges;
      // If the supertype is a concept defined by user
      // we just draw the isa edge instead of all edges from relationshipTypes
      if (!META_CONCEPTS.has(supLabel)) {
        edges = [{ from: type.id, to: sup.id, label: 'sub' }];
      } else {
        edges = await typeInboundEdges(type, state.visFacade);
      }
      const label = await type.label();

      let nodes = [Object.assign(type, { label })];

      // constuct role edges
      edges.push(await relationshipTypesOutboundEdges([type]));

      state.visFacade.addToCanvas({ nodes, edges });

      nodes = await computeAttributes(nodes);
      state.visFacade.updateNode(nodes);
      graknTx.close();
    })
      .then(() => {
        dispatch(UPDATE_METATYPE_INSTANCES);
      })
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
  },

  async [DEFINE_ROLE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    await state.schemaHandler.defineRole(payload);

    await Promise.all(payload.relationshipTypes.map(async (relationshipType) => {
      await state.schemaHandler.addRelatesRole({ label: relationshipType, roleLabel: payload.label });
    }));

    dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
  },

  async [DEFINE_ATTRIBUTE_TYPE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    // define entity type
    await state.schemaHandler.defineAttributeType(payload);

    // add attribute types to attribute type
    await Promise.all(payload.attributeTypes.map(async (attributeType) => {
      await state.schemaHandler.addAttribute({ schemaLabel: payload.attributeLabel, attributeLabel: attributeType });
    }));

    // add roles to attribute type
    await Promise.all(payload.roleTypes.map(async (roleType) => {
      await state.schemaHandler.addPlaysRole({ schemaLabel: payload.attributeLabel, roleLabel: roleType });
    }));

    dispatch(COMMIT_TX, graknTx)
      .then(() => {
        dispatch(UPDATE_METATYPE_INSTANCES);
      })
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
  },

  async [DEFINE_RELATIONSHIP_TYPE]({ state, dispatch }, payload) {
    let graknTx = await dispatch(OPEN_GRAKN_TX);
    await state.schemaHandler.defineRelationshipType(payload);

    // define and relate roles to relationship type
    await Promise.all(payload.defineRoles.map(async (roleType) => {
      await state.schemaHandler.defineRole({ roleLabel: roleType.label, superType: roleType.superType });
      await state.schemaHandler.addRelatesRole({ schemaLabel: payload.relationshipLabel, roleLabel: roleType.label });
    }));

    // relate roles to relationship type
    await Promise.all(payload.relateRoles.map(async (roleType) => {
      await state.schemaHandler.addRelatesRole({ schemaLabel: payload.relationshipLabel, roleLabel: roleType });
    }));

    // add attribute types to relationship type
    await Promise.all(payload.attributeTypes.map(async (attributeType) => {
      await state.schemaHandler.addAttribute({ schemaLabel: payload.relationshipLabel, attributeLabel: attributeType });
    }));

    // add roles to relationship type
    await Promise.all(payload.roleTypes.map(async (roleType) => {
      await state.schemaHandler.addPlaysRole({ schemaLabel: payload.relationshipLabel, roleLabel: roleType });
    }));

    dispatch(COMMIT_TX, graknTx).then(async () => {
      graknTx = await dispatch(OPEN_GRAKN_TX);
      const type = await graknTx.getSchemaConcept(payload.relationshipLabel);
      const label = await type.label();
      const sup = await type.sup();
      const supLabel = await sup.label();
      let edges;
      // If the supertype is a concept defined by user
      // we just draw the isa edge instead of all edges to roleplayers
      if (!META_CONCEPTS.has(supLabel)) {
        edges = [{ from: type.id, to: sup.id, label: 'isa' }];
      } else {
        const relatesEdges = await relationshipTypesOutboundEdges([type]);
        const plays = await typeInboundEdges(type, state.visFacade);
        edges = plays.concat(relatesEdges);
      }
      let nodes = [Object.assign(type, { label })];
      state.visFacade.addToCanvas({ nodes, edges });

      nodes = await computeAttributes(nodes);
      state.visFacade.updateNode(nodes);
      graknTx.close();
    })
      .then(() => {
        dispatch(UPDATE_METATYPE_INSTANCES);
      })
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
  },


  async [DELETE_SCHEMA_CONCEPT]({ state, dispatch, commit }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    const type = await graknTx.getSchemaConcept(payload.label);


    const instances = await (await type.instances()).collect();
    debugger;
    Promise.all(instances.map(async (thing) => {
      await thing.delete();
    }));


    if (payload.baseType === 'RELATIONSHIP_TYPE') {
      let roles = await (await type.roles()).collect();
      await Promise.all(roles.map(async (role) => {
        await state.schemaHandler.deleteRelatesRole({ label: payload.label, roleLabel: await role.label() });
      }));

      roles = await (await type.roles()).collect();
      debugger;
    }

    const typeId = await state.schemaHandler.deleteType(payload);

    debugger;

    dispatch(COMMIT_TX, graknTx).then(() => {
      state.visFacade.deleteFromCanvas([typeId]);
      commit('selectedNodes', null);
    })
      .catch((e) => {
        debugger;
        throw e;
      });
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
