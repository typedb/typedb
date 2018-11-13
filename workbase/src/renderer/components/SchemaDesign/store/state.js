import VisStyle from '../Style';

export default {
  metaTypeInstances: {},
  visStyle: VisStyle,
  visFacade: undefined,
  currentKeyspace: null,
  selectedNodes: null,
  loadingScehma: false,
  graknSession: undefined,
  canvasData: { entities: 0, attributes: 0, relationships: 0 },
  contextMenu: { show: false, x: null, y: null },
};
