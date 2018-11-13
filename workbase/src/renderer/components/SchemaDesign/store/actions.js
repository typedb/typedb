import {
  OPEN_GRAKN_TX,
  LOAD_SCHEMA,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
  UPDATE_METATYPE_INSTANCES,
} from '@/components/shared/StoresActions';

import Grakn from 'grakn';

import {
  computeSubConcepts,
  relationshipTypesOutboundEdges,
  updateNodePositions,
  loadMetaTypeInstances,
} from '../SchemaUtils';

export default {

  [OPEN_GRAKN_TX]({ state }) {
    return state.graknSession.transaction(Grakn.txType.WRITE);
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
    const response = await graknTx.execute('match $x sub thing; get;');
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
    graknTx.close();
    nodes = updateNodePositions(nodes);

    state.visFacade.addToCanvas({ nodes, edges: relEdges.concat(subConcepts.edges) });
    state.visFacade.fitGraphToWindow();
    commit('loadingSchema', false);
  },
};
