import Grakn from 'grakn';
import Vue from 'vue';
import logger from '@/../Logger';
import CanvasStoreMixin from '../shared/CanvasStoreMixin';
import VisualiserUtils from './VisualiserUtils';
import DisplaySettings from './RightBar/SettingsTab/DisplaySettings';
import QuerySettings from './RightBar/SettingsTab/QuerySettings';
import Style from './Style';
import VisualiserGraphBuilder from '../Visualiser/VisualiserGraphBuilder';


import {
  RUN_CURRENT_QUERY,
  EXPLAIN_CONCEPT,
  TOGGLE_LABEL,
  TOGGLE_COLOUR,
  LOAD_METATYPE_INSTANCES,
} from '../shared/StoresActions';

const actions = {

  async [RUN_CURRENT_QUERY]() {
    return this.runQuery(this.currentQuery);
  },
  async [EXPLAIN_CONCEPT]() {
    const saveloadRolePlayersState = QuerySettings.getRolePlayersStatus();

    QuerySettings.setRolePlayersStatus(true);

    const graknTx = await this.openGraknTx();

    this.explanationQuery = true;

    const queries = this.getSelectedNode().explanation.answers().map((answer) => {
      const queryPattern = answer.explanation().queryPattern();

      let query = VisualiserUtils.buildExplanationQuery(answer, queryPattern).query;
      if (queryPattern.includes('has')) {
        query += `${VisualiserUtils.buildExplanationQuery(answer, queryPattern).attributeQuery} get;`;
      } else {
        query += `$r ${queryPattern.slice(1, -1).match(/\((.*?;)/)[0]} offset 0; limit 1; get $r;`;
      }
      return query;
    });
    for (const q of queries) { // eslint-disable-line no-restricted-syntax
      const data = await this.runQuery(q, false);// eslint-disable-line no-await-in-loop
      const updatedEdges = data.edges.map(edge => Object.assign(edge, Style.computeExplanationEdgeStyle()));
      this.visFacade.container.visualiser.updateEdge(updatedEdges);
    }
    this.explanationQuery = false;
    graknTx.close();
    QuerySettings.setRolePlayersStatus(saveloadRolePlayersState);
  },
  async [TOGGLE_LABEL](type) {
    const graknTx = await this.openGraknTx();
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type).map(x => this.getNode(x.id, graknTx)));
    const updatedNodes = await VisualiserGraphBuilder.prepareNodes(nodes);
    this.visFacade.container.visualiser.updateNode(updatedNodes);
    graknTx.close();
  },
  async [TOGGLE_COLOUR](type) {
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type));
    const updatedNodes = nodes.map(node => Object.assign(node, Style.computeNodeStyle(node)));
    this.visFacade.container.visualiser.updateNode(updatedNodes);
  },
  async [LOAD_METATYPE_INSTANCES]() {
    const graknTx = await this.openGraknTx();

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

    this.metaTypeInstances = metaTypeInstances;
    graknTx.close();
  },
};

const watch = {
  isInit() {
    this.registerCanvasEventHandler('doubleClick', async (params) => {
      const nodeId = params.nodes[0];
      if (!nodeId) return;

      const neighboursLimit = QuerySettings.getNeighboursLimit();
      const visNode = this.visFacade.getNode(nodeId);

      if (params.event.srcEvent.shiftKey) { // shift + double click => load attributes
        this.loadAttributes(visNode, neighboursLimit);
      } else { // double click => load neighbours
        this.loadNeighbours(visNode, neighboursLimit);
      }
    });
  },
  currentKeyspace(newKs, oldKs) {
    if (newKs && newKs !== oldKs) {
      this.currentQuery = '';
    }
  },
};

const methods = {
  async runQuery(query, limitRoleplayers) {
    try {
      if (this.loadingQuery && !this.explanationQuery) return null; // Don't run query if previous is still running or explanation query
      query = query.trim();
      if (/^(.*;)\s*(delete\b.*;)$/.test(query) || /^(.*;)\s*(delete\b.*;)$/.test(query)
        || /^insert/.test(query)
        || /^(.*;)\s*(aggregate\b.*;)$/.test(query) || /^(.*;)\s*(aggregate\b.*;)$/.test(query)
        || (/^compute/.test(query) && !query.startsWith('compute path'))) {
        throw new Error('Only get and compute path queries are supported for now.');
      }

      this.loadingQuery = true;

      const graknTx = await this.openGraknTx();

      const result = (await (await graknTx.query(query)).collect());

      if (!result.length) {
        this.$notifyInfo('No results were found for your query!');
        this.loadingQuery = false;
        return null;
      }

      let data;

      if (this.isConceptMap(result)) {
        data = await VisualiserGraphBuilder.buildFromConceptMap(result, limitRoleplayers);
      } else { // result is conceptList
        // TBD - handle multiple paths
        const path = result[0];
        const pathNodes = await Promise.all(path.list().map(id => this.getNode(id, graknTx)));
        data = await VisualiserGraphBuilder.buildFromConceptList(path, pathNodes);
      }

      this.visFacade.addToCanvas(data);
      this.visFacade.fitGraphToWindow();
      this.updateCanvasData();

      data.nodes = await VisualiserUtils.computeAttributes(data.nodes);

      this.visFacade.updateNode(data.nodes);

      this.loadingQuery = false;

      graknTx.close();

      return data;
    } catch (e) {
      logger.error(e.stack);
      this.loadingQuery = false;
      throw e;
    }
  },

  async loadAttributes(visNode, neighboursLimit) {
    const query = `match $x id "${visNode.id}" has attribute $y; offset ${visNode.attrOffset}; limit ${neighboursLimit}; get $y;`;
    this.visFacade.updateNode({ id: visNode.id, attrOffset: visNode.attrOffset + neighboursLimit });

    const data = await this.runQuery(query);

    if (data) { // when attributes are fount, construct edges and add to graph
      const edges = data.nodes.map(attr => ({ from: visNode.id, to: attr.id, label: 'has' }));

      this.visFacade.addToCanvas({ nodes: data.nodes, edges });
      this.updateCanvasData();
    }
  },
  async loadNeighbours(visNode, neighboursLimit) {
    const query = VisualiserUtils.getNeighboursQuery(visNode, neighboursLimit);
    this.loadingQuery = true;
    this.visFacade.updateNode({ id: visNode.id, offset: (visNode.offset + neighboursLimit) });

    const graknTx = await this.openGraknTx();
    const result = (await (await graknTx.query(query)).collect());
    if (!result.length) {
      // this.$notifyInfo('No results were found for your query!');
      return;
    }

    const filteredResult = await VisualiserGraphBuilder.filterMaps(result);

    if (result.length !== filteredResult.length) {
      const offsetDiff = result.length - filteredResult.length;
      visNode.offset += QuerySettings.getNeighboursLimit();
      this.loadNeighbours(visNode, offsetDiff);
      if (!filteredResult.length) return;
    }


    const data = await VisualiserGraphBuilder.buildFromConceptMap(filteredResult);

    this.visFacade.addToCanvas(data);
    this.visFacade.fitGraphToWindow();
    this.updateCanvasData();

    const nodesWithAttribtues = await VisualiserUtils.computeAttributes(data.nodes);

    this.visFacade.updateNode(nodesWithAttribtues);

    this.loadingQuery = false;

    // when neighbours are found construct edges and add to graph
    let edges = [];

    if (visNode.baseType === 'ATTRIBUTE') {
      // Build edges to owners with label `has`
      edges = data.nodes.map(owner => ({ from: owner.id, to: visNode.id, label: 'has' }));
    } else if (visNode.baseType === 'RELATIONSHIP') {
      const roleplayersIds = data.nodes.map(x => x.id);
      const graknTx = await this.openGraknTx();
      const relationshipConcept = await this.getNode(visNode.id, graknTx);
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
    this.visFacade.addToCanvas({ nodes: data.nodes, edges });
    this.updateCanvasData();
  },

  // getters
  async openGraknTx() {
    return this.graknSession.transaction(Grakn.txType.WRITE);
  },
  getMetaTypeInstances() {
    return this.metaTypeInstances;
  },
  getLabelBySelectedType() {
    return DisplaySettings.getTypeLabels(this.getSelectedNode().type);
  },
  getCurrentQuery() {
    return this.currentQuery;
  },
  getVisStyle() {
    return Style;
  },
  showSpinner() {
    return this.loadingQuery || this.explanationQuery;
  },
  isConceptMap(result) {
    return (result[0].map);
  },

  // setters
  setCurrentQuery(query) {
    this.currentQuery = query;
  },
};

const state = {
  currentQuery: '',
  loadingQuery: false,
  explanationQuery: false,
  metaTypeInstances: {},
};
export default { create: () => new Vue({
  name: 'DataManagementStore',
  mixins: [CanvasStoreMixin.create()],
  data() { return Object.assign(state, { actions }); },
  methods,
  watch,
}),
};

