import QuerySettings from './RightBar/SettingsTab/QuerySettings';
import { LOAD_ATTRIBUTES, LOAD_NEIGHBOURS } from '../shared/StoresActions';

export default {
  registerHandlers({ state, dispatch, commit }, id) {
    commit('registerCanvasEvent', {
      id,
      event: 'selectNode',
      callback: (params) => {
        commit('selectedNodes', { id, nodeIds: params.nodes });
        commit('contextMenu', { id, show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'dragStart',
      callback: (params) => {
        if (!params.nodes.length > 1) {
          commit('selectedNodes', { id, nodeIds: [params.nodes[0]] });
        }
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'click',
      callback: (params) => {
        if (!params.nodes.length) { commit('selectedNodes', { id, nodeIDs: null }); }
        commit('contextMenu', { id, show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'oncontext',
      callback: (params) => {
        const nodeId = state.tabs[id].visFacade.getNetwork().getNodeAt(params.pointer.DOM);
        if (nodeId) {
          if (!(params.nodes.length > 1)) {
            state.tabs[id].visFacade.getNetwork().unselectAll();
            commit('selectedNodes', { id, nodeIds: [nodeId] });
            state.tabs[id].visFacade.getNetwork().selectNodes([nodeId]);
          }
        } else if (!(params.nodes.length > 1)) {
          commit('selectedNodes', { id, nodeIds: null });
          state.tabs[id].visFacade.getNetwork().unselectAll();
        }
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'oncontext',
      callback: (params) => {
        // Show context menu when keyspace is selected and canvas has data
        if (state.tabs[id].currentKeyspace && (state.tabs[id].canvasData.entities || state.tabs[id].canvasData.attributes || state.tabs[id].canvasData.relationships)) {
          commit('contextMenu', { id, show: true, x: params.pointer.DOM.x, y: params.pointer.DOM.y });
        }
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'doubleClick',
      callback: async (params) => {
        const nodeId = params.nodes[0];
        if (!nodeId) return;

        const neighboursLimit = QuerySettings.getNeighboursLimit();
        const visNode = state.tabs[id].visFacade.getNode(nodeId);
        const action = (params.event.srcEvent.shiftKey) ? LOAD_ATTRIBUTES : LOAD_NEIGHBOURS;
        dispatch(action, { id, visNode, neighboursLimit });
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'deselectNode',
      callback: () => {
        commit('contextMenu', { id, show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'dragStart',
      callback: () => {
        commit('contextMenu', { id, show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'zoom',
      callback: () => {
        commit('contextMenu', { id, show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      id,
      event: 'hold',
      callback: (params) => {
        if (params.nodes.length) { commit('selectedNodes', { id, nodeIds: null }); state.tabs[id].visFacade.getNetwork().unselectAll(); }
      },
    });
  },
};
