

export default {
  currentQuery: state => state.currentQuery,
  currentKeyspace: state => state.currentKeyspace,
  metaTypeInstances: state => state.metaTypeInstances,
  showSpinner: state => state.loadingQuery,
  selectedNodes: state => state.selectedNodes,
  selectedNode: state => ((state.selectedNodes) ? state.selectedNodes[0] : null),
  canvasData: state => state.canvasData,
  isActive: state => (state.currentKeyspace !== null),
};
