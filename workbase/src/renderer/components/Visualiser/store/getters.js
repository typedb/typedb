

export default {
  currentQuery: state => id => state.tabs[id].currentQuery,
  currentKeyspace: state => id => state.tabs[id].currentKeyspace,
  metaTypeInstances: state => id => state.tabs[id].metaTypeInstances,
  showSpinner: state => id => state.tabs[id].loadingQuery,
  selectedNodes: state => id => state.tabs[id].selectedNodes,
  selectedNode: state => id => ((state.tabs[id].selectedNodes) ? state.tabs[id].selectedNodes[0] : null),
  canvasData: state => id => state.tabs[id].canvasData,
  isActive: state => id => (state.tabs[id].currentKeyspace !== null),
  contextMenu: state => id => state.tabs[id].contextMenu,
};
