export default {
  currentQuery(state, { id, query }) {
    state.tabs[id].currentQuery = query;
  },
  currentKeyspace(state, { id, keyspace }) {
    state.tabs[id].currentKeyspace = keyspace;
  },
  loadingQuery(state, { id, isRunning }) {
    state.tabs[id].loadingQuery = isRunning;
  },
  graknSession(state, { id, session }) {
    state.tabs[id].graknSession = session;
  },
  setVisFacade(state, { id, facade }) {
    state.tabs[id].visFacade = Object.freeze(facade); // Freeze it so that Vue does not attach watchers to its properties
  },
  selectedNodes(state, { id, nodeIds }) {
    state.tabs[id].selectedNodes = (nodeIds) ? state.visFacade.getNode(nodeIds) : null;
  },
  metaTypeInstances(state, { id, instances }) {
    state.tabs[id].metaTypeInstances = instances;
  },
  registerCanvasEvent(state, { id, event, callback }) {
    state.tabs[id].visFacade.registerEventHandler(event, callback);
  },
  updateCanvasData(state, id) {
    if (state.tabs[id].visFacade) {
      state.tabs[id].canvasData = {
        entities: state.tabs[id].visFacade.getAllNodes().filter(x => x.baseType === 'ENTITY').length,
        attributes: state.tabs[id].visFacade.getAllNodes().filter(x => x.baseType === 'ATTRIBUTE').length,
        relationships: state.tabs[id].visFacade.getAllNodes().filter(x => x.baseType === 'RELATIONSHIP').length };
    }
  },
  contextMenu(state, { id, contextMenu }) {
    state.tabs[id].contextMenu = contextMenu;
  },
  addTab: (state, { id, tab }) => {
    state.tabs[id] = tab;
  },
};
