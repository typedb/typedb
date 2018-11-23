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
  [INITIALISE_VISUALISER]({ state, commit, dispatch }, { container, visFacade }) {
    addResetGraphListener(dispatch, CANVAS_RESET);
    commit('setVisFacade', visFacade.initVisualiser(container, state.visStyle));
    VisualiserCanvasEventsHandler.registerHandlers({ state, commit, dispatch });
  },

  [CANVAS_RESET]({ state, commit }) {
    state.visFacade.resetCanvas();
    commit('selectedNodes', null);
    commit('updateCanvasData');
  },

  [CURRENT_KEYSPACE_CHANGED]({ state, dispatch, commit, rootState }, keyspace) {
    if (keyspace !== state.currentKeyspace) {
      dispatch(CANVAS_RESET);
      commit('setCurrentQuery', '');
      commit('currentKeyspace', keyspace);
      commit('graknSession', rootState.grakn.session(keyspace));
      dispatch(UPDATE_METATYPE_INSTANCES);
    }
  },

  async [UPDATE_METATYPE_INSTANCES]({ dispatch, commit }) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const metaTypeInstances = await loadMetaTypeInstances(graknTx);
    graknTx.close();
    commit('metaTypeInstances', metaTypeInstances);
  },

  [OPEN_GRAKN_TX]({ state }) {
    return state.graknSession.transaction(Grakn.txType.WRITE);
  },

  async [UPDATE_NODES_LABEL]({ state, dispatch }, type) {
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const nodes = await Promise.all(state.visFacade.getAllNodes().filter(x => x.type === type).map(x => graknTx.getConcept(x.id)));
    const updatedNodes = await VisualiserGraphBuilder.prepareNodes(nodes);
    state.visFacade.updateNode(updatedNodes);
    graknTx.close();
  },

  [UPDATE_NODES_COLOUR]({ state }, type) {
    const nodes = state.visFacade.getAllNodes().filter(x => x.type === type);
    const updatedNodes = nodes.map(node => Object.assign(node, state.visStyle.computeNodeStyle(node)));
    state.visFacade.updateNode(updatedNodes);
  },

  async [LOAD_NEIGHBOURS]({ state, commit, dispatch }, { visNode, neighboursLimit }) {
    commit('loadingQuery', true);
    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const data = await getNeighboursData(visNode, graknTx, neighboursLimit);
    visNode.offset += neighboursLimit;
    state.visFacade.updateNode(visNode);
    state.visFacade.addToCanvas(data);
    if (data.nodes.length) state.visFacade.fitGraphToWindow();
    commit('updateCanvasData');
    const labelledNodes = await VisualiserGraphBuilder.prepareNodes(data.nodes);
    state.visFacade.updateNode(labelledNodes);
    const nodesWithAttribtues = await computeAttributes(data.nodes);
    state.visFacade.updateNode(nodesWithAttribtues);
    graknTx.close();
    commit('loadingQuery', false);
  },

  async [RUN_CURRENT_QUERY]({ state, dispatch, commit }) {
    try {
      const query = state.currentQuery;
      validateQuery(query);
      commit('loadingQuery', true);
      const graknTx = await dispatch(OPEN_GRAKN_TX);
      const result = (await (await graknTx.query(query)).collect());

      if (!result.length) {
        // this.$notifyInfo('No results were found for your query!');
        commit('loadingQuery', false);
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

      state.visFacade.addToCanvas(data);
      state.visFacade.fitGraphToWindow();
      commit('updateCanvasData');

      data.nodes = await computeAttributes(data.nodes);

      state.visFacade.updateNode(data.nodes);

      commit('loadingQuery', false);

      graknTx.close();

      return data;
    } catch (e) {
      logger.error(e.stack);
      commit('loadingQuery', false);
      throw e;
    }
  },
  async [LOAD_ATTRIBUTES]({ state, commit, dispatch }, { visNode, neighboursLimit }) {
    const query = `match $x id "${visNode.id}" has attribute $y; offset ${visNode.attrOffset}; limit ${neighboursLimit}; get $y;`;
    state.visFacade.updateNode({ id: visNode.id, attrOffset: visNode.attrOffset + neighboursLimit });

    const graknTx = await dispatch(OPEN_GRAKN_TX);
    const result = await (await graknTx.query(query)).collect();
    const autoloadRolePlayers = QuerySettings.getRolePlayersStatus();
    const data = await VisualiserGraphBuilder.buildFromConceptMap(result, autoloadRolePlayers, false);
    state.visFacade.addToCanvas(data);
    data.nodes = await computeAttributes(data.nodes);
    state.visFacade.updateNode(data.nodes);
    commit('loadingQuery', false);
    graknTx.close();

    if (data) { // when attributes are found, construct edges and add to graph
      const edges = data.nodes.map(attr => ({ from: visNode.id, to: attr.id, label: 'has' }));

      state.visFacade.addToCanvas({ nodes: data.nodes, edges });
      commit('updateCanvasData');
    }
  },
  async [EXPLAIN_CONCEPT]({ state, dispatch, getters, commit }) {
    const explanation = getters.selectedNode.explanation;

    let queries;
    // If the explanation is formed from a conjuction inside a rule, go one step deeper to access the actual explanation
    if (!explanation.queryPattern().length) {
      queries = explanation.answers().map(answer => answer.explanation().answers().map(answer => mapAnswerToExplanationQuery(answer))).flatMap(x => x);
    } else {
      queries = getters.selectedNode.explanation.answers().map(answer => mapAnswerToExplanationQuery(answer));
    }
    queries.forEach(async (query) => {
      commit('loadingQuery', true);
      const graknTx = await dispatch(OPEN_GRAKN_TX);
      const result = (await (await graknTx.query(query)).collect());

      const data = await VisualiserGraphBuilder.buildFromConceptMap(result, true, false);
      state.visFacade.addToCanvas(data);
      commit('updateCanvasData');
      const nodesWithAttributes = await computeAttributes(data.nodes);
      graknTx.close();

      state.visFacade.updateNode(nodesWithAttributes);
      const styledEdges = data.edges.map(edge => Object.assign(edge, state.visStyle.computeExplanationEdgeStyle()));
      state.visFacade.updateEdge(styledEdges);
      commit('loadingQuery', false);
    });
  },

  async [DELETE_SELECTED_NODES]({ state, commit }) {
    state.selectedNodes.forEach((node) => {
      state.visFacade.deleteNode(node);
    });
    commit('selectedNodes', null);
  },
};
