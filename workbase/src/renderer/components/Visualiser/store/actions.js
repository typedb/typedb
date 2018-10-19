import {
  RUN_CURRENT_QUERY,
  EXPLAIN_CONCEPT,
  UPDATE_NODES_LABEL,
  UPDATE_NODES_COLOUR,
  UPDATE_METATYPE_INSTANCES,
  INITIALISE_VISUALISER,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
  DELETE_SELECTED_NODES,
  OPEN_GRAKN_TX,
  LOAD_NEIGHBOURS,
  LOAD_ATTRIBUTES,
} from '@/components/shared/StoresActions';
import Grakn from 'grakn';
import logger from '@/../Logger';

import {
  addResetGraphListener,
  loadMetaTypeInstances,
  validateQuery,
  computeAttributes,
  mapAnswerToExplanationQuery,
  getNeighboursData } from '../VisualiserUtils';
import QuerySettings from '../RightBar/SettingsTab/QuerySettings';
import VisualiserGraphBuilder from '../VisualiserGraphBuilder';
import VisualiserCanvasEventsHandler from '../VisualiserCanvasEventsHandler';

export default {
  [INITIALISE_VISUALISER]({ state, commit, dispatch }, { id, container, visFacade }) {
    addResetGraphListener(id, dispatch, CANVAS_RESET);
    commit('setVisFacade', { id, facade: visFacade.initVisualiser(container, state.tabs[id].visStyle) });
    VisualiserCanvasEventsHandler.registerHandlers({ state, commit, dispatch }, id);
  },

  [CANVAS_RESET]({ state, commit }, id) {
    state.tabs[id].visFacade.resetCanvas();
    commit('selectedNodes', { id, nodeIds: null });
    commit('updateCanvasData', id);
  },

  [CURRENT_KEYSPACE_CHANGED]({ state, dispatch, commit, rootState }, { id, keyspace }) {
    if (keyspace !== state.tabs[id].currentKeyspace) {
      dispatch(CANVAS_RESET, id);
      commit('currentQuery', { id, query: '' });
      commit('currentKeyspace', { id, keyspace });
      commit('graknSession', { id, session: rootState.grakn.session(keyspace) });
      dispatch(UPDATE_METATYPE_INSTANCES, id);
    }
  },

  async [UPDATE_METATYPE_INSTANCES]({ dispatch, commit }, id) {
    const graknTx = await dispatch(OPEN_GRAKN_TX, id);
    const metaTypeInstances = await loadMetaTypeInstances(graknTx);
    graknTx.close();
    commit('metaTypeInstances', { id, instances: metaTypeInstances });
  },

  [OPEN_GRAKN_TX]({ state }, id) {
    return state.tabs[id].graknSession.transaction(Grakn.txType.WRITE);
  },

  async [UPDATE_NODES_LABEL]({ state, dispatch }, { id, type }) {
    const graknTx = await dispatch(OPEN_GRAKN_TX, id);
    const nodes = await Promise.all(state.tabs[id].visFacade.getAllNodes().filter(x => x.type === type).map(x => graknTx.getConcept(x.id)));
    const updatedNodes = await VisualiserGraphBuilder.prepareNodes(nodes);
    state.tabs[id].visFacade.updateNode(updatedNodes);
    graknTx.close();
  },

  [UPDATE_NODES_COLOUR]({ state }, { id, type }) {
    const nodes = state.tabs[id].visFacade.getAllNodes().filter(x => x.type === type);
    const updatedNodes = nodes.map(node => Object.assign(node, state.tabs[id].visStyle.computeNodeStyle(node)));
    state.tabs[id].visFacade.updateNode(updatedNodes);
  },

  async [LOAD_NEIGHBOURS]({ state, commit, dispatch }, { id, visNode, neighboursLimit }) {
    commit('loadingQuery', { id, isRunning: true });
    const graknTx = await dispatch(OPEN_GRAKN_TX, id);
    const data = await getNeighboursData(visNode, graknTx, neighboursLimit);
    visNode.offset += neighboursLimit;
    state.tabs[id].visFacade.updateNode(visNode);
    state.tabs[id].visFacade.addToCanvas(data);
    if (data.nodes.length) state.tabs[id].visFacade.fitGraphToWindow();
    commit('updateCanvasData', id);
    const labelledNodes = await VisualiserGraphBuilder.prepareNodes(data.nodes);
    state.tabs[id].visFacade.updateNode(labelledNodes);
    const nodesWithAttribtues = await computeAttributes(data.nodes);
    state.tabs[id].visFacade.updateNode(nodesWithAttribtues);
    graknTx.close();
    commit('loadingQuery', { id, isRunning: false });
  },

  async [RUN_CURRENT_QUERY]({ state, dispatch, commit }, id) {
    try {
      const query = state.tabs[id].currentQuery;
      validateQuery(query);
      commit('loadingQuery', { id, isRunning: true });
      const graknTx = await dispatch(OPEN_GRAKN_TX, id);
      const result = (await (await graknTx.query(query)).collect());

      if (!result.length) {
        // this.$notifyInfo('No results were found for your query!');
        commit('loadingQuery', { id, isRunning: false });
        return null;
      }

      let data;
      if (result[0].map) {
        const autoloadRolePlayers = QuerySettings.getRolePlayersStatus();
        data = await VisualiserGraphBuilder.buildFromConceptMap(result, autoloadRolePlayers, true);
      } else { // result is conceptList
        // TBD - handle multiple paths
        const path = result[0];
        const pathNodes = await Promise.all(path.list().map(id => graknTx.getConcept(id)));
        data = await VisualiserGraphBuilder.buildFromConceptList(path, pathNodes);
      }

      state.tabs[id].visFacade.addToCanvas(data);
      state.tabs[id].visFacade.fitGraphToWindow();
      commit('updateCanvasData', id);

      data.nodes = await computeAttributes(data.nodes);

      state.tabs[id].visFacade.updateNode(data.nodes);

      commit('loadingQuery', { id, isRunning: false });

      graknTx.close();

      return data;
    } catch (e) {
      logger.error(e.stack);
      commit('loadingQuery', { id, isRunning: false });
      throw e;
    }
  },
  async [LOAD_ATTRIBUTES]({ state, commit, dispatch }, { id, visNode, neighboursLimit }) {
    const query = `match $x id "${visNode.id}" has attribute $y; offset ${visNode.attrOffset}; limit ${neighboursLimit}; get $y;`;
    state.tabs[id].visFacade.updateNode({ id: visNode.id, attrOffset: visNode.attrOffset + neighboursLimit });

    const graknTx = await dispatch(OPEN_GRAKN_TX, id);
    const result = await (await graknTx.query(query)).collect();
    const autoloadRolePlayers = QuerySettings.getRolePlayersStatus();
    const data = await VisualiserGraphBuilder.buildFromConceptMap(result, autoloadRolePlayers, false);
    state.tabs[id].visFacade.addToCanvas(data);
    data.nodes = await computeAttributes(data.nodes);
    state.tabs[id].visFacade.updateNode(data.nodes);
    commit('loadingQuery', { id, isRunning: false });
    graknTx.close();

    if (data) { // when attributes are found, construct edges and add to graph
      const edges = data.nodes.map(attr => ({ from: visNode.id, to: attr.id, label: 'has' }));

      state.tabs[id].visFacade.addToCanvas({ nodes: data.nodes, edges });
      commit('updateCanvasData', id);
    }
  },
  async [EXPLAIN_CONCEPT]({ state, dispatch, getters, commit }, id) {
    const queries = getters.selectedNode(id).explanation.answers().map(answer => mapAnswerToExplanationQuery(answer));
    queries.forEach(async (query) => {
      commit('loadingQuery', { id, isRunning: true });
      const graknTx = await dispatch(OPEN_GRAKN_TX, id);
      const result = (await (await graknTx.query(query)).collect());

      const data = await VisualiserGraphBuilder.buildFromConceptMap(result, true, false);
      state.tabs[id].visFacade.addToCanvas(data);
      commit('updateCanvasData', id);
      const nodesWithAttributes = await computeAttributes(data.nodes);
      graknTx.close();

      state.tabs[id].visFacade.updateNode(nodesWithAttributes);
      const styledEdges = data.edges.map(edge => Object.assign(edge, state.tabs[id].visStyle.computeExplanationEdgeStyle()));
      state.tabs[id].visFacade.updateEdge(styledEdges);
      commit('loadingQuery', { id, isRunning: false });
    });
  },

  async [DELETE_SELECTED_NODES]({ state, commit }, id) {
    state.tabs[id].selectedNodes.forEach((node) => {
      state.tabs[id].visFacade.deleteNode(node);
    });
    commit('selectedNodes', { id, nodeIds: null });
  },
};
