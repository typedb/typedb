import Grakn from 'grakn';
import Vue from 'vue';
import logger from '@/../Logger';
import CanvasStoreMixin from '../shared/CanvasStoreMixin/CanvasStoreMixin';
import ManagementUtils from './VisualiserUtils';
import NodeSettings from './RightBar/SettingsTab/DisplaySettings';
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
    await this.openGraknTx();

    this.explanationQuery = true;

    const queries = this.getSelectedNode().explanation.answers().map((answer) => {
      const queryPattern = answer.explanation().queryPattern();

      let query = ManagementUtils.buildExplanationQuery(answer, queryPattern).query;
      if (queryPattern.includes('has')) {
        query += `${ManagementUtils.buildExplanationQuery(answer, queryPattern).attributeQuery} get;`;
      } else {
        query += `$r ${queryPattern.slice(1, -1).match(/\((.*?;)/)[0]} offset 0; limit 1; get $r;`;
      }
      return query;
    });
    for (const q of queries) { // eslint-disable-line no-restricted-syntax
      const data = await this.runQuery(q);// eslint-disable-line no-await-in-loop
      const updatedEdges = data.edges.map(edge => Object.assign(edge, Style.computeExplanationEdgeStyle()));
      this.visFacade.container.visualiser.updateEdge(updatedEdges);
    }

    this.explanationQuery = false;
    await this.closeGraknTx();
  },
  async [TOGGLE_LABEL](type) {
    await this.openGraknTx();
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type).map(x => this.getNode(x.id)));
    const updatedNodes = await VisualiserGraphBuilder.prepareNodes(nodes);
    this.visFacade.container.visualiser.updateNode(updatedNodes);
    await this.closeGraknTx();
  },
  async [TOGGLE_COLOUR](type) {
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type));
    const updatedNodes = nodes.map(node => Object.assign(node, Style.computeNodeStyle(node)));
    this.visFacade.container.visualiser.updateNode(updatedNodes);
  },
  async [LOAD_METATYPE_INSTANCES]() {
    await this.openGraknTx();

    // Fetch types
    const entities = await (await this.graknTx.query('match $x sub entity; get;')).collectConcepts();
    const rels = await (await this.graknTx.query('match $x sub relationship; get;')).collectConcepts();
    const attributes = await (await this.graknTx.query('match $x sub attribute; get;')).collectConcepts();
    const roles = await (await this.graknTx.query('match $x sub role; get;')).collectConcepts();

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

    await this.closeGraknTx();
    this.metaTypeInstances = metaTypeInstances;
  },
};

const watch = {
  isInit() {
    this.registerCanvasEventHandler('doubleClick', async (params) => {
      const nodeId = params.nodes[0];
      if (!nodeId) return;

      const neighboursLimit = QuerySettings.getNeighboursLimit();
      const visNode = this.visFacade.getNode(nodeId);

      let query;
      if (params.event.srcEvent.shiftKey) { // shift + double click => load attributes
        query = `match $x id "${nodeId}" has attribute $y; offset ${visNode.attrOffset}; limit ${neighboursLimit}; get;`;
        this.visFacade.updateNode({ id: nodeId, attrOffset: visNode.attrOffset + neighboursLimit });
      } else { // double click => load neighbours
        query = ManagementUtils.loadNeighbours(visNode, neighboursLimit);
        this.visFacade.updateNode({ id: nodeId, offset: visNode.offset + neighboursLimit });
      }
      this.setCurrentQuery(query);
      this.runQuery(query);
    });
  },
  currentKeyspace(newKs, oldKs) {
    if (newKs && newKs !== oldKs) {
      this.currentQuery = '';
    }
  },
};

const methods = {
  async runQuery(query) {
    try {
      if (this.loadingQuery && !this.explanationQuery) return null; // Don't run query if previous is still running or explanation query
      if (/^(.*;)\s*(delete\b.*;)$/.test(query) || /^(.*;)\s*(delete\b.*;)$/.test(query) || /^insert/.test(query)) {
        throw new Error('The transaction is read only - insert and delete queries are not supported');
      }
      this.loadingQuery = true;

      await this.openGraknTx();

      const result = (await (await this.graknTx.query(query)).collect());

      if (!result.length) {
        this.$notifyInfo('No results were found for your query!', 'bottom-right');
        this.loadingQuery = false;
        return null;
      }

      let data;

      if (this.isConceptMap(result)) {
        data = await VisualiserGraphBuilder.buildFromConceptMap(result);
      } else { // result is conceptList
        // TBD - handle multiple paths
        const path = result[0];
        const pathNodes = await Promise.all(path.list().map(id => this.getNode(id)));
        data = await VisualiserGraphBuilder.buildFromConceptList(path, pathNodes);
      }

      this.visFacade.addToCanvas(data);
      this.visFacade.fitGraphToWindow();
      this.updateCanvasData();
      this.loadingQuery = false;

      await this.closeGraknTx();

      return data;
    } catch (e) {
      logger.error(e.stack);
      this.loadingQuery = false;
      throw e;
    }
  },
  async openGraknTx() {
    this.graknTx = await this.graknSession.transaction(Grakn.txType.WRITE);
  },
  async closeGraknTx() {
    await this.graknTx.close();
  },

  // getters
  getMetaTypeInstances() {
    return this.metaTypeInstances;
  },
  async getNode(nodeId) {
    return this.graknTx.getConcept(nodeId);
  },
  getLabelBySelectedType() {
    return NodeSettings.getTypeLabels(this.getSelectedNode().type);
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
  graknTx: undefined,
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

