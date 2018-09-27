import {
  RUN_CURRENT_QUERY,
  EXPLAIN_CONCEPT,
  TOGGLE_LABEL,
  TOGGLE_COLOUR,
  LOAD_METATYPE_INSTANCES,
  INITIALISE_VISUALISER,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
} from '@/components/shared/StoresActions';
import Grakn from 'grakn';
import logger from '@/../Logger';
import VisFacade from '@/components/CanvasVisualiser/Facade';

import VisualiserUtils from '../VisualiserUtils';
import QuerySettings from '../RightBar/SettingsTab/QuerySettings';
import VisualiserGraphBuilder from '../VisualiserGraphBuilder';
import VisualiserCanvasEventsHandler from '../VisualiserCanvasEventsHandler';
const LETTER_G_KEYCODE = 71;

const addResetGraphListener = dispatch => window.addEventListener('keydown', (e) => {
  // Reset canvas when metaKey(CtrlOrCmd) + G are pressed
  if ((e.keyCode === LETTER_G_KEYCODE) && e.metaKey) { dispatch(CANVAS_RESET); }
});

export default {
  [INITIALISE_VISUALISER]({ state, commit, dispatch }, container) {
    addResetGraphListener(dispatch);
    commit('setVisFacade', VisFacade.initVisualiser(container, state.visStyle));
    VisualiserCanvasEventsHandler.registerHandlers({ state, commit, dispatch });
  },
  [CANVAS_RESET]({ state, commit }) {
    state.visFacade.resetCanvas();
    commit('selectedNodes', null);
    commit('updateCanvasData');
  },
  async loadAttributes({ state, commit, dispatch }, { visNode, neighboursLimit }) {
    const query = `match $x id "${visNode.id}" has attribute $y; offset ${visNode.attrOffset}; limit ${neighboursLimit}; get $y;`;
    state.visFacade.updateNode({ id: visNode.id, attrOffset: visNode.attrOffset + neighboursLimit });

    const data = await dispatch('runQuery', { query });

    if (data) { // when attributes are fount, construct edges and add to graph
      const edges = data.nodes.map(attr => ({ from: visNode.id, to: attr.id, label: 'has' }));

      state.visFacade.addToCanvas({ nodes: data.nodes, edges });
      commit('updateCanvasData');
    }
  },
  async loadNeighbours({ state, commit, dispatch }, { visNode, neighboursLimit }) {
    const saveloadRolePlayersState = QuerySettings.getRolePlayersStatus();

    QuerySettings.setRolePlayersStatus(true);
    const query = VisualiserUtils.getNeighboursQuery(visNode, neighboursLimit);
    commit('loadingQuery', true);
    state.visFacade.updateNode({ id: visNode.id, offset: (visNode.offset + neighboursLimit) });

    const graknTx = await dispatch('openGraknTx');
    const result = (await (await graknTx.query(query)).collect());
    if (!result.length) {
      // this.$notifyInfo('No results were found for your query!');
      commit('loadingQuery', false);
      return;
    }
    const filteredResult = await VisualiserGraphBuilder.filterMaps(result);

    if (result.length !== filteredResult.length) {
      const offsetDiff = result.length - filteredResult.length;
      visNode.offset += QuerySettings.getNeighboursLimit();
      dispatch('loadNeighbours', { visNode, neighboursLimit: offsetDiff });
      if (!filteredResult.length) return;
    }


    const data = await VisualiserGraphBuilder.buildFromConceptMap(filteredResult, false);

    state.visFacade.addToCanvas(data);
    state.visFacade.fitGraphToWindow();
    commit('updateCanvasData');

    const nodesWithAttribtues = await VisualiserUtils.computeAttributes(data.nodes);

    state.visFacade.updateNode(nodesWithAttribtues);

    commit('loadingQuery', false);

    // when neighbours are found construct edges and add to graph
    let edges = [];

    if (visNode.baseType === 'ATTRIBUTE') {
      // Build edges to owners with label `has`
      edges = data.nodes.map(owner => ({ from: owner.id, to: visNode.id, label: 'has' }));
    } else if (visNode.baseType === 'RELATIONSHIP') {
      const roleplayersIds = data.nodes.map(x => x.id);
      const graknTx = await dispatch('openGraknTx');
      const relationshipConcept = await graknTx.getConcept(visNode.id);
      const roleplayers = Array.from((await relationshipConcept.rolePlayersMap()).entries());

      await Promise.all(Array.from(roleplayers, async ([role, setOfThings]) => {
        const roleLabel = await role.label();
        Array.from(setOfThings.values()).forEach((thing) => {
          if (roleplayersIds.includes(thing.id)) {
            edges.push({ from: visNode.id, to: thing.id, label: roleLabel });
          }
        });
      }));
      graknTx.close();
    }
    state.visFacade.addToCanvas({ nodes: data.nodes, edges });
    commit('updateCanvasData');
    QuerySettings.setRolePlayersStatus(saveloadRolePlayersState);
  },
  async [CURRENT_KEYSPACE_CHANGED]({ state, dispatch, commit, rootState }, keyspace) {
    if (keyspace !== state.currentKeyspace) {
      state.visFacade.resetCanvas();
      commit('updateCanvasData');
      commit('selectedNodes', null);
      commit('currentQuery', '');
      commit('currentKeyspace', keyspace);
      if (keyspace) { // keyspace will be null if user deletes current keyspace from Keyspaces page
        const graknSession = rootState.grakn.session(state.currentKeyspace);
        commit('graknSession', graknSession);
        dispatch(LOAD_METATYPE_INSTANCES);
      }
    }
  },
  [RUN_CURRENT_QUERY]({ state, dispatch }) {
    return dispatch('runQuery', { query: state.currentQuery });
  },
  async [EXPLAIN_CONCEPT]({ state, dispatch, getters }) {
    const saveloadRolePlayersState = QuerySettings.getRolePlayersStatus();
    QuerySettings.setRolePlayersStatus(true);

    const queries = getters.selectedNode.explanation.answers().map((answer) => {
      const queryPattern = answer.explanation().queryPattern();

      let query = VisualiserUtils.buildExplanationQuery(answer, queryPattern).query;
      if (queryPattern.includes('has')) {
        query += `${VisualiserUtils.buildExplanationQuery(answer, queryPattern).attributeQuery} get;`;
      } else {
        query += `$r ${queryPattern.slice(1, -1).match(/\((.*?;)/)[0]} offset 0; limit 1; get $r;`;
      }
      return query;
    });

    // TODO: parallelise this
    for (const query of queries) { // eslint-disable-line no-restricted-syntax
      const data = await dispatch('runQuery', { query, shouldLimitRoleplayers: false });// eslint-disable-line no-await-in-loop
      const updatedEdges = data.edges.map(edge => Object.assign(edge, state.visStyle.computeExplanationEdgeStyle()));
      state.visFacade.container.visualiser.updateEdge(updatedEdges);
    }
    QuerySettings.setRolePlayersStatus(saveloadRolePlayersState);
  },
  async [TOGGLE_LABEL]({ state, dispatch }, type) {
    const graknTx = await dispatch('openGraknTx');
    const nodes = await Promise.all(state.visFacade.getAllNodes().filter(x => x.type === type).map(x => graknTx.getConcept(x.id)));
    const updatedNodes = await VisualiserGraphBuilder.prepareNodes(nodes);
    state.visFacade.container.visualiser.updateNode(updatedNodes);
    graknTx.close();
  },
  async [TOGGLE_COLOUR](state, type) {
    const nodes = await Promise.all(state.visFacade.getAllNodes().filter(x => x.type === type));
    const updatedNodes = nodes.map(node => Object.assign(node, state.visStyle.computeNodeStyle(node)));
    state.visFacade.container.visualiser.updateNode(updatedNodes);
  },
  async [LOAD_METATYPE_INSTANCES]({ dispatch, commit }) {
    const graknTx = await dispatch('openGraknTx');

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

    commit('metaTypeInstances', metaTypeInstances);
    graknTx.close();
  },
  async runQuery({ state, dispatch, commit }, { query, shouldLimitRoleplayers }) {
    try {
      query = query.trim();
      if (/^(.*;)\s*(delete\b.*;)$/.test(query) || /^(.*;)\s*(delete\b.*;)$/.test(query)
        || /^insert/.test(query)
        || /^(.*;)\s*(aggregate\b.*;)$/.test(query) || /^(.*;)\s*(aggregate\b.*;)$/.test(query)
        || (/^compute/.test(query) && !query.startsWith('compute path'))) {
        throw new Error('Only get and compute path queries are supported for now.');
      }

      commit('loadingQuery', true);

      const graknTx = await dispatch('openGraknTx');
      const result = (await (await graknTx.query(query)).collect());

      if (!result.length) {
        this.$notifyInfo('No results were found for your query!');
        commit('loadingQuery', false);
        return null;
      }

      let data;

      if (result[0].map) {
        data = await VisualiserGraphBuilder.buildFromConceptMap(result, shouldLimitRoleplayers);
      } else { // result is conceptList
        // TBD - handle multiple paths
        const path = result[0];
        const pathNodes = await Promise.all(path.list().map(id => graknTx.getConcept(id)));
        data = await VisualiserGraphBuilder.buildFromConceptList(path, pathNodes);
      }

      state.visFacade.addToCanvas(data);
      state.visFacade.fitGraphToWindow();
      commit('updateCanvasData');

      data.nodes = await VisualiserUtils.computeAttributes(data.nodes);

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
  openGraknTx({ state }) {
    return state.graknSession.transaction(Grakn.txType.WRITE);
  },
};
