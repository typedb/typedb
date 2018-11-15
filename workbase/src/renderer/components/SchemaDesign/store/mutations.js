export default {
  currentKeyspace(state, keyspace) {
    state.currentKeyspace = keyspace;
  },
  loadingSchema(state, isRunning) {
    state.loadingSchema = isRunning;
  },
  graknSession(state, session) {
    state.graknSession = session;
  },
  setVisFacade(state, facade) {
    state.visFacade = Object.freeze(facade); // Freeze it so that Vue does not attach watchers to its properties
  },
  selectedNodes(state, nodeIds) {
    state.selectedNodes = (nodeIds) ? state.visFacade.getNode(nodeIds) : null;
  },
  metaTypeInstances(state, instances) {
    state.metaTypeInstances = instances;
  },
  registerCanvasEvent(state, { event, callback }) {
    state.visFacade.registerEventHandler(event, callback);
  },
  updateCanvasData(state) {
    if (state.visFacade) {
      state.canvasData = {
        entities: state.visFacade.getAllNodes().filter(x => x.baseType === 'ENTITY').length,
        attributes: state.visFacade.getAllNodes().filter(x => x.baseType === 'ATTRIBUTE').length,
        relationships: state.visFacade.getAllNodes().filter(x => x.baseType === 'RELATIONSHIP').length };
    }
  },
  setContextMenu(state, contextMenu) {
    state.contextMenu = contextMenu;
  },
};
