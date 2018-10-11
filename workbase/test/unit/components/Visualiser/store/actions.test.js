import Vue from 'vue';
import Vuex from 'vuex';

import {
  CANVAS_RESET,
  INITIALISE_VISUALISER,
  OPEN_GRAKN_TX,
  UPDATE_METATYPE_INSTANCES,
  UPDATE_NODES_LABEL,
  UPDATE_NODES_COLOUR,
  LOAD_NEIGHBOURS,
  DELETE_SELECTED_NODES,
  EXPLAIN_CONCEPT,
} from '@/components/shared/StoresActions';
import actions from '@/components/Visualiser/store/actions';
import mutations from '@/components/Visualiser/store/mutations';
import getters from '@/components/Visualiser/store/getters';

import { addResetGraphListener, loadMetaTypeInstances, getNeighboursData, computeAttributes } from '@/components/Visualiser/VisualiserUtils';
import VisualiserCanvasEventsHandler from '@/components/Visualiser/VisualiserCanvasEventsHandler';
import VisualiserGraphBuilder from '@/components/Visualiser/VisualiserGraphBuilder';

import MockConcepts from '../../../../helpers/MockConcepts';


jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));
jest.mock('@/../Logger', () => ({ error: () => {} }));
jest.mock('@/components/shared/PersistentStorage', () => {});
jest.mock('@/components/CanvasVisualiser/Facade', () => ({
  initVisualiser: () => ({ registerEventHandler: () => { } }),
}));

jest.mock('@/components/Visualiser/VisualiserUtils', () => ({
  addResetGraphListener: jest.fn(),
  loadMetaTypeInstances: jest.fn(),
  getNeighboursData: jest.fn().mockImplementation(() => Promise.resolve({ nodes: [{ id: 1234 }] })),
  computeAttributes: jest.fn(),
}));

jest.mock('@/components/Visualiser/VisualiserCanvasEventsHandler', () => ({
  registerHandlers: jest.fn(),
}));

jest.mock('@/components/Visualiser/VisualiserGraphBuilder', () => ({
  prepareNodes: jest.fn(),
}));

Vue.use(Vuex);

describe('actions', () => {
  let store;
  let setVisFacade;
  let registerCanvasEvent;
  let metaTypeInstances;
  let loadingQuery;

  beforeEach(() => {
    setVisFacade = jest.fn();
    registerCanvasEvent = jest.fn();
    metaTypeInstances = jest.fn();
    loadingQuery = jest.fn();


    store = new Vuex.Store({
      state: {
        visFacade: {
          resetCanvas: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
          updateNode: jest.fn(),
          addToCanvas: jest.fn(),
          deleteNode: jest.fn(),
          registerEventHandler: jest.fn(),
          fitGraphToWindow: jest.fn(),
        },
        selectedNodes: [MockConcepts.getMockEntity1()],
        canvasData: { entities: 2, attributes: 2, relationships: 2 },
        currentQuery: 'match $x isa person; get;',
        currentKeyspace: 'gene',
        metaTypeInstances: {},
        graknSession: { transaction: () => Promise.resolve({ close: jest.fn(), getConcept: () => Promise.resolve([{ id: 1234 }]) }) },
        visStyle: { computeNodeStyle: jest.fn() },
        loadingQuery: false,
      },
      mutations: {
        setVisFacade,
        registerCanvasEvent,
        metaTypeInstances,
        loadingQuery,
        selectedNodes: mutations.selectedNodes,
        updateCanvasData: mutations.updateCanvasData,
      },
      actions,
      getters,
    });
  });

  test('INITIALISE_VISUALISER', () => {
    const mockContainer = {};

    store.hotUpdate({
      mutations: { setVisFacade, registerCanvasEvent },
    });

    store.dispatch(INITIALISE_VISUALISER, mockContainer).then(() => {
      expect(addResetGraphListener).toHaveBeenCalled();
      expect(setVisFacade.mock.calls).toHaveLength(1);
      expect(VisualiserCanvasEventsHandler.registerHandlers).toHaveBeenCalled();
    });
  });

  test('CANVAS_RESET', () => store.dispatch(CANVAS_RESET).then(() => {
    expect(store.state.visFacade.resetCanvas).toHaveBeenCalled();
    expect(store.state.selectedNodes).toBe(null);
    expect(store.state.canvasData.entities).toBe(0);
    expect(store.state.canvasData.attributes).toBe(0);
    expect(store.state.canvasData.relationships).toBe(0);
  }));

  // test('CURRENT_KEYSPACE_CHANGED', () => {
  //   store.dispatch(CURRENT_KEYSPACE_CHANGED, 'pokemon').then(() => {
  //     expect(actions[CANVAS_RESET].mock.calls).toHaveLength(1);
  //     expect(store.state.currentQuery).toBe('');
  //     expect(store.state.currentKeyspace).toBe('pokemon');
  //     expect(actions[UPDATE_METATYPE_INSTANCES].mock.calls).toHaveLength(1);
  //   });
  // });

  test('UPDATE_METATYPE_INSTANCES', () => {
    store.dispatch(UPDATE_METATYPE_INSTANCES).then(() => {
      expect(loadMetaTypeInstances).toHaveBeenCalled();
      expect(metaTypeInstances.mock.calls).toHaveLength(1);
    });
  });

  // test('OPEN_GRAKN_TX', () => {
  //   store.hotUpdate({
  //     state: { graknSession: { transaction: () => jest.fn() },
  //     },
  //   });
  //
  //   store.dispatch(OPEN_GRAKN_TX).then(() => {
  //     expect(store.state.graknSession.transaction).toHaveBeenCalled();
  //   });
  // });

  test('UPDATE_NODES_LABEL', () => {
    store.dispatch(UPDATE_NODES_LABEL, 'person').then(() => {
      expect(store.state.visFacade.getAllNodes).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.prepareNodes).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
    });
  });

  test('UPDATE_NODES_COLOUR', () => {
    store.dispatch(UPDATE_NODES_COLOUR, 'person').then(() => {
      expect(store.state.visFacade.getAllNodes).toHaveBeenCalled();
      expect(store.state.visStyle.computeNodeStyle).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
    });
  });

  test('LOAD_NEIGHBOURS', () => {
    const mockVisNode = MockConcepts.getMockEntity1();

    store.hotUpdate({
      mutations: { loadingQuery, updateCanvasData: mutations.updateCanvasData,
      },
    });

    store.dispatch(LOAD_NEIGHBOURS, { visNode: mockVisNode, neighboursLimit: 1 }).then(() => {
      expect(loadingQuery.mock.calls).toHaveLength(2);
      expect(getNeighboursData).toHaveBeenCalled();
      expect(mockVisNode.offset).toBe(1);
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.prepareNodes).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
    });
  });

  test('DELETE_SELECTED_NODES', () => {
    store.dispatch(DELETE_SELECTED_NODES, 'person').then(() => {
      expect(store.state.visFacade.deleteNode).toHaveBeenCalledWith('1234');
      expect(store.state.selectedNodes).toBe(null);
    });
  });

  test('EXPLAIN_CONCEPT', () => {
    store.dispatch(EXPLAIN_CONCEPT);
  });
});
