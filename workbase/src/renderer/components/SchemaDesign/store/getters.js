export default {
  currentKeyspace: state => state.currentKeyspace,
  metaTypeInstances: state => state.metaTypeInstances,
  showSpinner: state => state.loadingSchema,
  selectedNodes: state => state.selectedNodes,
  selectedNode: state => ((state.selectedNodes) ? state.selectedNodes[0] : null),
  canvasData: state => state.canvasData,
  isActive: state => (state.currentKeyspace !== null),
  contextMenu: state => state.contextMenu,
  loadingSchema: state => state.loadingSchema,
  schemaHandler: state => state.schemaHandler,
};
