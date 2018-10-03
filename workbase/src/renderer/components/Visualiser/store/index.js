import actions from './actions';
import getters from './getters';
import mutations from './mutations';
import VisStyle from '../Style';

const state = {
  currentQuery: '',
  metaTypeInstances: {},
  visStyle: VisStyle,
  visFacade: undefined,
  currentKeyspace: null,
  selectedNodes: null,
  loadingQuery: false,
  graknSession: undefined,
  canvasData: { entities: 0, attributes: 0, relationships: 0 },
};

export default {
  state,
  getters,
  actions,
  mutations,
};
