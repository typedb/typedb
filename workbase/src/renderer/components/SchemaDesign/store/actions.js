import {
  OPEN_GRAKN_TX,
  LOAD_SCHEMA,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
  UPDATE_METATYPE_INSTANCES,
  INITIALISE_VISUALISER,
  DEFINE_ENTITY_TYPE,
  COMMIT_TX,
  DEFINE_ATTRIBUTE_TYPE,
  DEFINE_RELATIONSHIP_TYPE,
  DELETE_SCHEMA_CONCEPT,
  REFRESH_SELECTED_NODE,
  DELETE_ATTRIBUTE,
  DEFINE_RULE,
  ADD_ATTRIBUTE_TYPE,
} from '@/components/shared/StoresActions';
import logger from '@/../Logger';

import {
  META_CONCEPTS,
  relationshipTypesOutboundEdges,
  ownerHasEdges,
  computeSubConcepts,
} from '@/components/shared/SharedUtils';

import Grakn from 'grakn';
import SchemaHandler from '../SchemaHandler';
import {
  updateNodePositions,
  loadMetaTypeInstances,
  typeInboundEdges,
  computeAttributes,
} from '../SchemaUtils';
import SchemaCanvasEventsHandler from '../SchemaCanvasEventsHandler';

async function buildSchema(nodes) {
  // Find nodes that are subconcepts of existing types - these nodes will only have isa edges
  const subConcepts = await computeSubConcepts(nodes);

  // Draw all edges from relationships to roleplayers
  const relEdges = await relationshipTypesOutboundEdges(nodes);

  // Draw all edges from owners to attributes
  const hasEdges = await ownerHasEdges(nodes);

  return { nodes, edges: relEdges.concat(subConcepts.edges, hasEdges) };
}

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
        // .filter(x => !x.isAttributeType())
        .filter(x => x.label !== 'thing')
        .filter(x => x.label !== 'entity')
        .filter(x => x.label !== 'attribute')
        .filter(x => x.label !== 'relationship');

      const data = await buildSchema(nodes);

      data.nodes = updateNodePositions(data.nodes);

      state.visFacade.addToCanvas({ nodes: data.nodes, edges: data.edges });
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

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });

    await dispatch(UPDATE_METATYPE_INSTANCES);

    graknTx = await dispatch(OPEN_GRAKN_TX);

    const type = await graknTx.getSchemaConcept(payload.entityLabel);

    Object.assign(type, { label: payload.entityLabel });

    const data = await buildSchema([type]);

    const sup = await type.sup();
    if (sup) {
      const supLabel = await sup.label();
      if (META_CONCEPTS.has(supLabel)) {
        data.edges.push(...((await typeInboundEdges(type, state.visFacade)).filter(x => x.to === type.id)));
      }
    }

    state.visFacade.addToCanvas({ nodes: data.nodes, edges: data.edges });

    // attach attributes to visnode and update on graph to render the right bar attributes
    data.nodes = await computeAttributes(data.nodes);
    state.visFacade.updateNode(data.nodes);
    graknTx.close();
  },

  async [DEFINE_ATTRIBUTE_TYPE]({ state, dispatch }, payload) {
    let graknTx = await dispatch(OPEN_GRAKN_TX);

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

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });

    await dispatch(UPDATE_METATYPE_INSTANCES);

    graknTx = await dispatch(OPEN_GRAKN_TX);

    const type = await graknTx.getSchemaConcept(payload.attributeLabel);

    Object.assign(type, { label: payload.attributeLabel });

    const data = await buildSchema([type]);

    data.edges.push(...((await typeInboundEdges(type, state.visFacade)).filter(x => x.to === type.id)));

    state.visFacade.addToCanvas({ nodes: data.nodes, edges: data.edges });

    // attach attributes to visnode and update on graph to render the right bar attributes
    data.nodes = await computeAttributes(data.nodes);
    state.visFacade.updateNode(data.nodes);
    graknTx.close();
  },

  async [ADD_ATTRIBUTE_TYPE]({ state, dispatch }, payload) {
    let graknTx = await dispatch(OPEN_GRAKN_TX);

    // add attribute types to schema concept
    await Promise.all(payload.attributeTypes.map(async (attributeType) => {
      await state.schemaHandler.addAttribute({ schemaLabel: payload.label, attributeLabel: attributeType });
    }));

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
    graknTx = await dispatch(OPEN_GRAKN_TX);

    const node = state.visFacade.getNode(state.selectedNodes[0].id);

    const edges = await Promise.all(payload.attributeTypes.map(async (attributeType) => {
      const type = await graknTx.getSchemaConcept(attributeType);
      const dataType = await type.dataType();
      node.attributes = [...node.attributes, { type: attributeType, dataType }];

      return { from: state.selectedNodes[0].id, to: type.id, label: 'has' };
    }));

    state.visFacade.addToCanvas({ nodes: [], edges });

    graknTx.close();
    state.visFacade.updateNode(node);
  },

  async [DELETE_ATTRIBUTE]({ state, dispatch }, payload) {
    let graknTx = await dispatch(OPEN_GRAKN_TX);

    const type = await graknTx.getSchemaConcept(state.selectedNodes[0].label);

    if (await (await type.instances()).next()) throw Error('Cannot remove attribute type from schema concept with instances.');

    await state.schemaHandler.deleteAttribute(payload);

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });

    const node = state.visFacade.getNode(state.selectedNodes[0].id);
    node.attributes = Object.values(node.attributes).sort((a, b) => ((a.type > b.type) ? 1 : -1));
    node.attributes.splice(payload.index, 1);
    state.visFacade.updateNode(node);

    // delete edge to attribute type
    graknTx = await dispatch(OPEN_GRAKN_TX);

    const attributeTypeId = (await graknTx.getSchemaConcept(payload.attributeLabel)).id;
    const edgesIds = state.visFacade.edgesConnectedToNode(state.selectedNodes[0].id);

    edgesIds
      .filter(edgeId => (state.visFacade.getEdge(edgeId).to === attributeTypeId) && (state.visFacade.getEdge(edgeId).label === 'has'))
      .forEach((edgeId) => { state.visFacade.deleteEdge(edgeId); });

    graknTx.close();
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

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });

    await dispatch(UPDATE_METATYPE_INSTANCES);

    graknTx = await dispatch(OPEN_GRAKN_TX);

    const type = await graknTx.getSchemaConcept(payload.relationshipLabel);

    Object.assign(type, { label: payload.relationshipLabel });

    const data = await buildSchema([type]);

    data.edges.push(...((await typeInboundEdges(type, state.visFacade)).filter(x => x.to === type.id)));

    state.visFacade.addToCanvas({ nodes: data.nodes, edges: data.edges });

    // attach attributes to visnode and update on graph to render the right bar attributes
    data.nodes = await computeAttributes(data.nodes);
    state.visFacade.updateNode(data.nodes);
    graknTx.close();
  },

  async [DELETE_SCHEMA_CONCEPT]({ state, dispatch, commit }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    const type = await graknTx.getSchemaConcept(payload.label);

    const subs = await (await type.subs()).collect();
    await Promise.all(subs.map(async (x) => {
      if (await x.label() !== payload.label) throw Error('Cannot delete sub-typed schema concept');
    }));

    if (await (await type.instances()).next()) throw Error('Cannot delete schema concept which has instances');

    if (payload.baseType === 'RELATIONSHIP_TYPE') {
      const roles = await (await type.roles()).collect();
      await Promise.all(roles.map(async (role) => {
        const roleLabel = await role.label();
        const rolePlayers = await (await role.players()).collect();

        await Promise.all(rolePlayers.map(async (player) => {
          await state.schemaHandler.deletePlaysRole({ label: await player.label(), roleLabel });
        }));

        // If relationship type is suptyped and inherits a role only unrelate the role
        // otherwise unrelate and delete role
        const sup = await type.sup();
        if (sup) {
          const supLabel = await sup.label();
          if (!META_CONCEPTS.has(supLabel)) { // check if relationship type is sub-typed or not
            const roleSup = await role.sup();
            const roleSupLabel = await roleSup.label();
            if (roleSupLabel === 'role') await type.unrelate(role); // check if role is overridden or not
            else await state.schemaHandler.deleteRelatesRole({ label: payload.label, roleLabel });
          } else {
            await state.schemaHandler.deleteRelatesRole({ label: payload.label, roleLabel });
          }
        }
      }));
    } else if (payload.baseType === 'ATTRIBUTE_TYPE') {
      const nodes = state.visFacade.getAllNodes();
      await Promise.all(nodes.map(async (node) => {
        await state.schemaHandler.deleteAttribute({ label: node.label, attributeLabel: payload.label });
        node.attributes = node.attributes.filter((x => x.type !== payload.label));
      }));
      state.visFacade.updateNode(nodes);
    }

    const typeId = await state.schemaHandler.deleteType(payload);

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
    state.visFacade.deleteFromCanvas([typeId]);
    commit('selectedNodes', null);
    await dispatch(UPDATE_METATYPE_INSTANCES);
  },

  [REFRESH_SELECTED_NODE]({ state, commit }) {
    const node = state.selectedNodes[0];
    if (!node) return;
    commit('selectedNodes', null);
    commit('selectedNodes', [node.id]);
  },

  async [DEFINE_RULE]({ state, dispatch }, payload) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);

    // define rule
    await state.schemaHandler.defineRule(payload);

    await dispatch(COMMIT_TX, graknTx)
      .catch((e) => {
        graknTx.close();
        logger.error(e.stack);
        throw e;
      });
  },

  // async [DELETE_PLAYS_ROLE]({ state, dispatch }, payload) {
  //   const graknTx = await dispatch(OPEN_GRAKN_TX);
  //   await state.schemaHandler.deletePlaysRole(payload);
  //   dispatch(COMMIT_TX, graknTx).then(async () => {
  //     const type = await graknTx.getSchemaConcept(payload.label);
  //     state.visFacade.deleteEdgesOnNode(type.id, payload.roleName);
  //     dispatch(REFRESH_SELECTED_NODE);
  //   })
  //     .catch((e) => { throw e; });
  // },

  // async [DELETE_RELATES_ROLE]({ state, dispatch }, payload) {
  //   const graknTx = await dispatch(OPEN_GRAKN_TX);
  //   await state.schemaHandler.deleteRelatesRole(payload);
  //   dispatch(COMMIT_TX, graknTx).then(async () => {
  //     const type = await graknTx.getSchemaConcept(payload.label);
  //     state.visFacade.deleteEdgesOnNode(type.id, payload.roleName);
  //     dispatch(REFRESH_SELECTED_NODE);
  //   })
  //     .catch((e) => { throw e; });
  // },

  // async [ADD_TYPE]({ dispatch, state }, payload) {
  //   const graknTx = await dispatch(OPEN_GRAKN_TX);

  //   switch (payload.type) {
  //     case 'attribute': {
  //       await this.schemaHandler.addAttribute({ label: state.selectedNodes[0].label, typeLabel: payload.typeLabel });
  //       break;
  //     }
  //     case 'plays': {
  //       await this.schemaHandler.addPlaysRole({ label: state.selectedNodes[0].label, typeLabel: payload.typeLabel });
  //       const type = await graknTx.getSchemaConcept(state.selectedNodes[0].label);
  //       const relatesEdges = await relationshipTypesOutboundEdges([type]);
  //       state.visFacade.addToCanvas({ nodes: [], edges: relatesEdges });
  //       break;
  //     }
  //     case 'relates': {
  //       await this.schemaHandler.addRelatesRole({ label: state.selectedNodes[0].label, typeLabel: payload.typeLabel });
  //       const type = await graknTx.getSchemaConcept(state.selectedNodes[0].label);
  //       const relatesEdges = await relationshipTypesOutboundEdges([type]);
  //       state.visFacade.addToCanvas({ nodes: [], edges: relatesEdges });
  //       break;
  //     }
  //     default:
  //       // do nothing
  //   }
  //   dispatch(REFRESH_SELECTED_NODE);
  // },
};
