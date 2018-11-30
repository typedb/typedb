import storage from '@/components/shared/PersistentStorage';

export default {
  registerHandlers({ commit, state }) {
    commit('registerCanvasEvent', {
      event: 'selectNode',
      callback: (params) => {
        commit('selectedNodes', params.nodes);
        commit('setContextMenu', { show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      event: 'dragStart',
      callback: (params) => {
        if (!params.nodes.length > 1) {
          commit('selectedNodes', [params.nodes[0]]);
        }
      },
    });

    commit('registerCanvasEvent', {
      event: 'hold',
      callback: (params) => {
        if (params.nodes.length) { commit('selectedNodes', null); state.visFacade.getNetwork().unselectAll(); }
      },
    });

    commit('registerCanvasEvent', {
      event: 'click',
      callback: (params) => {
        if (!params.nodes.length) { commit('selectedNodes', null); }
        commit('setContextMenu', { show: false, x: null, y: null });
      },
    });

    commit('registerCanvasEvent', {
      event: 'dragEnd',
      callback: (params) => {
        if (!params.nodes.length) return;
        let positionMap = storage.get('schema-node-positions');

        if (positionMap) {
          positionMap = JSON.parse(positionMap);
        } else {
          positionMap = {};
          storage.set('schema-node-positions', {});
        }
        params.nodes.forEach((nodeId) => {
          positionMap[nodeId] = params.pointer.canvas;
        });
        storage.set('schema-node-positions', JSON.stringify(positionMap));
      },
    });
  },
};
