

import GlobalStore from '@/store';
import VisFacade from '@/components/CanvasVisualiser/Facade';

// Import actions
import {
  INITIALISE_VISUALISER,
  CURRENT_KEYSPACE_CHANGED,
  CANVAS_RESET,
  LOAD_METATYPE_INSTANCES,
} from './StoresActions';

const eventCache = {};

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
    this.registerCanvasEventHandlers();
    this.registerVueCanvasEventHandlers();
    // Register caches events

    Object.keys(eventCache).forEach((x) => {
      this.registerCanvasEventHandler(x, eventCache[x]);
    });
  },
  [CANVAS_RESET]() {
    this.visFacade.resetCanvas();
    this.setSelectedNodes(null);
    this.updateCanvasData();
  },
};

const methods = {
  // Executes actions on other modules that are not getters or setters.
  dispatch(event, payload) { return this.actions[event].call(this, payload); },
  registerCanvasEventHandlers() {
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

  // Getters
  getNode(nodeId, graknTx) { return graknTx.getConcept(nodeId); },
  getCurrentKeyspace() { return this.currentKeyspace; },
  getSelectedNode() { return (this.selectedNodes) ? this.selectedNodes[0] : null; },
  getSelectedNodes() { return this.selectedNodes; },
  isActive() { return this.currentKeyspace !== null; },

  // Setters
  setSelectedNodes(nodeIds) { this.selectedNodes = (nodeIds) ? this.visFacade.getNode(nodeIds) : null; },
  setSelectedNode(nodeId) { this.selectedNodes = (nodeId) ? [this.visFacade.getNode(nodeId)] : null; },
  registerCanvasEventHandler(event, fn) {
    if (this.visFacade) { debugger; this.visFacade.registerEventHandler(event, fn); } else {
      eventCache[event] = fn;
    }
  },

  updateCanvasData() {
    if (this.visFacade) {
      this.canvasData = {
        entities: this.visFacade.getAllNodes().filter(x => x.baseType === 'ENTITY').length,
        attributes: this.visFacade.getAllNodes().filter(x => x.baseType === 'ATTRIBUTE').length,
        relationships: this.visFacade.getAllNodes().filter(x => x.baseType === 'RELATIONSHIP').length };
    }
  },
};

const state = {
  visFacade: undefined,
  currentKeyspace: null,
  selectedNodes: null,
  actions,
  canvasData: { entities: 0, attributes: 0, relationships: 0 },
};

export default {
  create: () => ({
    name: 'CanvasStore',
    data() { return state; },
    methods,
  }),
};

