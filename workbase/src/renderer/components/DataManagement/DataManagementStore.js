import Vue from 'vue';
import logger from '@/../Logger';
import CanvasStoreMixin from '../shared/CanvasStoreMixin/CanvasStoreMixin';
import ManagementUtils from './DataManagementUtils';
import NodeSettings from './DataManagementContent/NodeSettingsPanel/NodeSettings';
import QuerySettings from './DataManagementContent/MenuBar/QuerySettings/QuerySettings';
import Style from './Style';
import VisualiserGraphBuilder from './VisualiserGraphBuilder';


import { RUN_CURRENT_QUERY, EXPLAIN_CONCEPT, TOGGLE_LABEL, TOGGLE_COLOUR } from '../shared/StoresActions';

const actions = {

  async [RUN_CURRENT_QUERY]() {
    return this.runQuery(this.currentQuery);
  },
  async [EXPLAIN_CONCEPT]() {
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
  },
  async [TOGGLE_LABEL](type) {
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type).map(x => this.getNode(x.id)));
    const updatedNodes = await VisualiserGraphBuilder.prepareNodes(nodes);
    this.visFacade.container.visualiser.updateNode(updatedNodes);
  },
  async [TOGGLE_COLOUR](type) {
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type));
    const updatedNodes = nodes.map(node => Object.assign(node, Style.computeNodeStyle(node)));
    this.visFacade.container.visualiser.updateNode(updatedNodes);
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

      return data;
    } catch (e) {
      logger.error(e.stack);
      this.loadingQuery = false;
      // Every time an excepion occurs, the current graknTx will be closed by the server, so open a new one
      if (this.graknSession) { this.openGraknTx(); }
      throw e;
    }
  },

  // getters
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
};
export default { create: () => new Vue({
  name: 'DataManagementStore',
  mixins: [CanvasStoreMixin.create()],
  data() { return Object.assign(state, { actions }); },
  methods,
  watch,
}),
};

