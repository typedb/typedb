import Vue from 'vue';
import storage from '@/components/shared/PersistentStorage';
import logger from '@/../Logger';
import CanvasStoreMixin from '../shared/CanvasStoreMixin/CanvasStoreMixin';
import ManagementUtils from './VisualiserUtils';
import NodeSettings from './RightBar/NodeSettings';
import Style from './Style';

import { RUN_CURRENT_QUERY, TOGGLE_LABEL, TOGGLE_COLOUR } from '../shared/StoresActions';

const actions = {

  async [RUN_CURRENT_QUERY]() {
    if (/^compute path/.test(this.currentQuery)) return this.computeShortestPath(this.currentQuery);
    return this.runQuery(this.currentQuery);
  },
  async [TOGGLE_LABEL](type) {
    const nodes = await Promise.all(this.visFacade.getAllNodes().filter(x => x.type === type).map(x => this.getNode(x.id)));
    const updatedNodes = await ManagementUtils.prepareNodes(nodes);
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
      this.loadingQuery = true;
      const node = await this.getNode(nodeId);
      const neighboursLimit = storage.get('neighbours_limit');
      const visNode = this.visFacade.getNode(nodeId);

      let data;
      if (params.event.srcEvent.shiftKey) {
        data = await ManagementUtils.loadAttributes(node, neighboursLimit, visNode.attrOffset);
        this.visFacade.updateNode({ id: nodeId, attrOffset: visNode.attrOffset + neighboursLimit });
      } else {
        data = await ManagementUtils.loadNeighbours(node, neighboursLimit, visNode.offset);
        this.visFacade.updateNode({ id: nodeId, offset: visNode.offset + neighboursLimit });
      }

      this.visFacade.addToCanvas(data);
      this.updateCanvasData();
      this.loadingQuery = false;
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
      if (this.loadingQuery && !this.explanationQuery) return; // Don't run query if previous is still running or explanation query
      if (/^(.*;)\s*(delete\b.*;)$/.test(query) || /^(.*;)\s*(delete\b.*;)$/.test(query) || /^insert/.test(query)) {
        throw new Error('The transaction is read only - insert and delete queries are not supported');
      }
      this.loadingQuery = true;

      const concepts = await this.exectueQuery(query);

      const data = { nodes: await ManagementUtils.prepareNodes(concepts), edges: [] };

      if (ManagementUtils.isRolePlayerAutoloadEnabled()) {
        const relationships = data.nodes.filter(x => x.baseType === 'RELATIONSHIP');
        const roleplayers = await ManagementUtils.relationshipsRolePlayers(relationships, true, storage.get('neighbours_limit'));

        relationships.map((rel) => {
          rel.offset += storage.get('neighbours_limit');
          return rel;
        });

        data.nodes.push(...roleplayers.nodes);
        data.edges.push(...roleplayers.edges);
      }


      this.visFacade.addToCanvas(data);
      this.visFacade.fitGraphToWindow();

      if (this.explanationQuery) {
        const updatedEdges = data.edges.map(edge => Object.assign(edge, Style.computeExplanationEdgeStyle()));
        this.visFacade.container.visualiser.updateEdge(updatedEdges);
      } else {
        this.loadingQuery = false;
      }
      this.updateCanvasData();
    } catch (e) {
      logger.error(e.stack);
      this.loadingQuery = false;
      // Every time an excepion occurs, the current graknTx will be closed by the server, so open a new one
      if (this.graknSession) { this.openGraknTx(); }
      throw e;
    }
  },
  async exectueQuery(query) {
    const result = (await (await this.graknTx.query(query)).collect());
    const concepts = await ManagementUtils.prepareConcepts(result);

    if (!concepts.length) {
      this.$notifyInfo('No results were found for your query!', 'bottom-right');
    }
    return ManagementUtils.filterImplicitTypes(concepts);
  },
  getLabelBySelectedType() { return NodeSettings.getTypeLabels(this.getSelectedNode().type); },

  // getters
  getCurrentQuery() {
    return this.currentQuery;
  },
  // setters
  setCurrentQuery(query) {
    this.currentQuery = query;
  },
  showSpinner() { return this.loadingQuery; },
  getVisStyle() { return Style; },
  async explainConcept(node) {
    this.explanationQuery = true;

    const queries = node.explanation.answers().map((answer) => {
      const queryPattern = answer.explanation().queryPattern();

      let query = this.buildQuery(answer, queryPattern).query;
      if (queryPattern.includes('has')) {
        query += `${this.buildQuery(answer, queryPattern).attributeQuery} get;`;
      } else {
        query += `$r ${queryPattern.slice(1, -1).match(/\((.*?;)/)[0]} offset 0; limit 1; get $r;`;
      }
      return query;
    });
    for (const q of queries) { // eslint-disable-line no-restricted-syntax
      await this.runQuery(q);// eslint-disable-line no-await-in-loop
    }
    this.loadingQuery = false;
    this.explanationQuery = false;
  },
  buildQuery(answer, queryPattern) {
    let query = 'match ';
    let attributeQuery = null;
    Array.from(answer.map().entries()).forEach(([graqlVar, concept]) => {
      if (concept.isAttribute()) {
        attributeQuery = `has ${queryPattern.match(/(?:has )(\w+)/)[1]} $${graqlVar};`;
      } else if (concept.isEntity()) {
        query += `$${graqlVar} id ${concept.id}; `;
      }
    });
    return { query, attributeQuery };
  },
  async computeShortestPath(query) {
    // TBD - handle multiple paths
    this.loadingQuery = true;
    const path = await (await this.graknTx.query(query)).next();

    const nodes = await Promise.all(path.list().map(id => this.getNode(id)));

    const data = { nodes: await ManagementUtils.prepareNodes(nodes), edges: [] };

    const relationships = data.nodes.filter(x => x.baseType === 'RELATIONSHIP');

    const roleplayers = await ManagementUtils.relationshipsRolePlayers(relationships);

    roleplayers.nodes.filter(x => path.list().includes(x.id));
    roleplayers.edges.filter(x => (path.list().includes(x.to) && path.list().includes(x.to)));

    data.nodes.push(...roleplayers.nodes);
    data.edges.push(...roleplayers.edges);


    this.visFacade.addToCanvas(data);

    const updatedEdges = data.edges.map(edge => Object.assign(edge, Style.computeShortestPathEdgeStyle()));
    this.visFacade.container.visualiser.updateEdge(updatedEdges);

    this.visFacade.fitGraphToWindow();
    this.updateCanvasData();
    this.loadingQuery = false;
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

