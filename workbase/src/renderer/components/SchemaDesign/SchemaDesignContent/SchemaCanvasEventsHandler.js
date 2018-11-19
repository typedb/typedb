import storage from '@/components/shared/PersistentStorage';

export default {
  registerHandlers({ commit }) {
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
