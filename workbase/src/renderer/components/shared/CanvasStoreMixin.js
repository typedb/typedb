

import GlobalStore from '@/store';
import VisFacade from '@/components/CanvasVisualiser/Facade';

// Import actions
import {
  INITIALISE_VISUALISER,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
  LOAD_METATYPE_INSTANCES,
} from './StoresActions';

const actions = {
  async [CURRENT_KEYSPACE_CHANGED](keyspace) {
    if (keyspace !== this.currentKeyspace) {
      this.visFacade.resetCanvas();
      this.updateCanvasData();
      this.setSelectedNodes(null);
      this.currentKeyspace = keyspace;
      if (keyspace) { // keyspace will be null if user deletes current keyspace from Keyspaces page
        this.graknSession = GlobalStore.state.grakn.session(this.currentKeyspace);
        this.openGraknTx();
        this.dispatch(LOAD_METATYPE_INSTANCES);
      }
    }
  },
  [INITIALISE_VISUALISER](container) {
    // We freeze visualiser facade so that Vue does not attach watchers to its properties
    this.visFacade = Object.freeze(VisFacade.initVisualiser(container, this.getVisStyle()));
    // Now that the visualiser is initialised it's possible to register events on the network
    this.isInit = true;
  },
  [CANVAS_RESET]() {
    this.visFacade.resetCanvas();
    this.setSelectedNodes(null);
    this.updateCanvasData();
  },
};

const watch = {
  isInit() {
    this.registerCanvasEventHandler('selectNode', (params) => {
      this.setSelectedNodes(params.nodes);
    });
    this.registerCanvasEventHandler('oncontext', (params) => {
      const nodeId = this.visFacade.container.visualiser.getNetwork().getNodeAt(params.pointer.DOM);
      if (nodeId) {
        if (!(params.nodes.length > 1)) {
          this.visFacade.container.visualiser.getNetwork().unselectAll();
          this.setSelectedNodes([nodeId]);
          this.visFacade.container.visualiser.getNetwork().selectNodes([nodeId]);
        }
      } else if (!(params.nodes.length > 1)) {
        this.setSelectedNodes(null);
        this.visFacade.container.visualiser.getNetwork().unselectAll();
      }
    });
    this.registerCanvasEventHandler('dragStart', (params) => {
      if (!params.nodes.length > 1) { this.setSelectedNodes([params.nodes[0]]); }
    });
    this.registerCanvasEventHandler('click', (params) => {
      if (!params.nodes.length) { this.setSelectedNodes(null); }
    });
  },
};

const methods = {
  // Executes actions on other modules that are not getters or setters.
  dispatch(event, payload) { return this.actions[event].call(this, payload); },

  // Getters
  getNode(nodeId, graknTx) { return graknTx.getConcept(nodeId); },
  getCurrentKeyspace() { return this.currentKeyspace; },
  getSelectedNode() { return (this.selectedNodes) ? this.selectedNodes[0] : null; },
  getSelectedNodes() { return this.selectedNodes; },
  isActive() { return this.currentKeyspace !== null; },

  // Setters
  setSelectedNodes(nodeIds) { this.selectedNodes = (nodeIds) ? this.visFacade.getNode(nodeIds) : null; },
  setSelectedNode(nodeId) { this.selectedNodes = (nodeId) ? [this.visFacade.getNode(nodeId)] : null; },
  registerCanvasEventHandler(event, fn) { this.visFacade.registerEventHandler(event, fn); },

  updateCanvasData() { if (this.visFacade) this.canvasData = { nodes: this.visFacade.getAllNodes().length, edges: this.visFacade.getAllEdges().length }; },
};

const state = {
  visFacade: undefined,
  currentKeyspace: null,
  selectedNodes: null,
  isInit: false,
  actions,
  canvasData: { nodes: 0, edges: 0 },
};

export default {
  create: () => ({
    name: 'CanvasStore',
    data() { return state; },
    methods,
    watch,
  }),
};

