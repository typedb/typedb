import DataManagementStore from '@/components/Visualiser/VisualiserStore.js';
import { CURRENT_KEYSPACE_CHANGED, INITIALISE_VISUALISER, CANVAS_RESET } from '@/components/shared/StoresActions';

jest.mock('@/../Logger', () => ({ error: () => {} }));
jest.mock('@/components/Visualiser/VisualiserUtils.js', () => ({
  limitQuery: () => {},
  isRolePlayerAutoloadEnabled: () => false,
  prepareNodes: () => ([]),
}));
jest.mock('@/components/Visualiser/RightBar/SettingsTab/DisplaySettings.js', () => {});
jest.mock('@/components/CanvasVisualiser/Facade', () => ({
  initVisualiser: () => ({ registerEventHandler: () => { } }),
}));

jest.mock('grakn', () => ({
  txType: { WRITE: 'write' },

}));

jest.mock('@/components/EngineSettings', () => ({ getEngineGrpcUri: () => 'localhost:48555' }));
jest.mock('@/components/shared/PersistentStorage', () => {});
jest.mock('@/store', () => ({
  state: { grakn: { session: () => ({ transaction: () => Promise.resolve({ execute: jest.fn().mockImplementation(() => Promise.resolve([])) }) }) } },
}));


let store;
beforeEach(() => {
  store = DataManagementStore.create();
});

describe('DataManagementStore', () => {
  test('When action initialise-visualiser, isInit is true', () => {
    expect(store.$data.isInit).toBeFalsy();
    store.dispatch(INITIALISE_VISUALISER);
    expect(store.$data.isInit).toBeTruthy();
  });
  describe('actions', () => {
    test('Run query when current Query is empty', async () => {
      store.graknTx = { execute: jest.fn() };
      store.currentKeyspace = 'grakn';
      store.$nextTick(async () => {
        await store.dispatch('run-query');
        expect(store.graknTx.execute).not.toHaveBeenCalled();
      });
    });
    test('Run query when currentQuery not empty and keyspace selected - graknTx.execute correctly invoked', () => {
      store.visFacade = { addToCanvas: () => {}, fitGraphToWindow: () => {} };
      store.currentQuery = 'match $x isa thing; get;';
      store.currentKeyspace = 'grakn';
      store.$nextTick(async () => {
        await store.dispatch('run-query');
        expect(store.graknTx.execute).toHaveBeenCalledWith('match $x isa thing; get;');
      });
    });
  });
});

describe('CavasStoreMixin', () => {
  describe('actions', () => {
    test('current keyspace changed', () => {
      store.visFacade = { resetCanvas: jest.fn(), getAllNodes: () => [], getAllEdges: () => [] };
      store.setSelectedNodes = jest.fn();
      store.currentKeyspace = 'old-keyspace';
      store.openGraknTx = jest.fn();

      store.dispatch(CURRENT_KEYSPACE_CHANGED, 'new-keyspace');

      expect(store.setSelectedNodes).toHaveBeenCalledWith(null);
      expect(store.currentKeyspace).toBe('new-keyspace');
      expect(store.graknSession).toBeDefined();
      expect(store.openGraknTx).toHaveBeenCalled();
      expect(store.visFacade.resetCanvas).toBeCalled();
    });
    test('reset cavas', () => {
      store.visFacade = { resetCanvas: jest.fn(), getAllNodes: () => [], getAllEdges: () => [] };
      store.setSelectedNodes = jest.fn();

      store.dispatch(CANVAS_RESET);
      expect(store.visFacade.resetCanvas).toHaveBeenCalled();
      expect(store.setSelectedNodes).toHaveBeenCalledWith(null);
    });
  });
  describe('methods', () => {
    const mockNode = { id: '321' };

    test('get current keyspace', () => {
      store.currentKeyspace = 'test-keyspace';
      expect(store.getCurrentKeyspace()).toBe('test-keyspace');
    });
    test('get selected nodes', () => {
      store.selectedNodes = mockNode;
      expect(store.getSelectedNodes()).toBe(mockNode);
    });
    test('isActive', () => {
      store.currentKeyspace = true;
      expect(store.isActive()).toBe(true);
    });
    test('set selected node', () => {
      store.visFacade = { getNode: () => mockNode };
      store.setSelectedNode(mockNode);
      expect(store.selectedNodes).toEqual([mockNode]);
    });
    test('register canvas event handler', () => {
      store.visFacade.registerEventHandler = jest.fn();
      store.registerCanvasEventHandler('mock-event', 'mock-fn');
      expect(store.visFacade.registerEventHandler).toHaveBeenCalledWith('mock-event', 'mock-fn');
    });
    test('get node', () => {
      store.graknTx.getConcept = jest.fn();
      store.getNode(mockNode.id);
      expect(store.graknTx.getConcept).toHaveBeenCalledWith(mockNode.id);
    });
  });
});
