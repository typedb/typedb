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
  LOAD_ATTRIBUTES,
  RUN_CURRENT_QUERY,
  CURRENT_KEYSPACE_CHANGED,
  EXPLAIN_CONCEPT,
} from '@/components/shared/StoresActions';
import actions from '@/components/Visualiser/store/actions';
import mutations from '@/components/Visualiser/store/mutations';
import getters from '@/components/Visualiser/store/getters';

import { addResetGraphListener, loadMetaTypeInstances, getNeighboursData, computeAttributes, validateQuery, mapAnswerToExplanationQuery } from '@/components/Visualiser/VisualiserUtils';
import VisualiserCanvasEventsHandler from '@/components/Visualiser/VisualiserCanvasEventsHandler';
import VisualiserGraphBuilder from '@/components/Visualiser/VisualiserGraphBuilder';
import QuerySettings from '@/components/Visualiser/RightBar/SettingsTab/QuerySettings';


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
  validateQuery: jest.fn(),
  mapAnswerToExplanationQuery: jest.fn().mockImplementation(() => '{$y id 1234; $r (father: $m, role: $y) isa parentship; $m id 4444;}'),
  getNeighboursData: jest.fn().mockImplementation(() => Promise.resolve({ nodes: [{ id: 1234 }] })),
  computeAttributes: jest.fn().mockImplementation(() => Promise.resolve([MockConcepts.getMockEntity1()])),
}));

jest.mock('@/components/Visualiser/VisualiserCanvasEventsHandler', () => ({
  registerHandlers: jest.fn(),
}));

jest.mock('@/components/Visualiser/VisualiserGraphBuilder', () => ({
  prepareNodes: jest.fn(),
  buildFromConceptMap: jest.fn().mockImplementation(() => Promise.resolve({ nodes: [MockConcepts.getMockEntity1()], edges: [{ from: 1234, to: 4321, label: 'son' }] })),
}));

jest.mock('@/components/Visualiser/RightBar/SettingsTab/QuerySettings', () => ({
  getRolePlayersStatus: jest.fn(),
}));

Vue.use(Vuex);

let store;
let loadingQuery;
let mockVisNode;
let setVisFacade;
let registerCanvasEvent;
let metaTypeInstances;

beforeEach(() => {
  setVisFacade = jest.fn();
  registerCanvasEvent = jest.fn();
  metaTypeInstances = jest.fn();
  loadingQuery = jest.fn();
  mockVisNode = MockConcepts.getMockEntity1();

  store = new Vuex.Store({
    state: {
      visFacade: {
        resetCanvas: jest.fn(),
        getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        updateNode: jest.fn(),
        updateEdge: jest.fn(),
        addToCanvas: jest.fn(),
        deleteNode: jest.fn(),
        registerEventHandler: jest.fn(),
        fitGraphToWindow: jest.fn(),
      },
      selectedNodes: [MockConcepts.getMockRelationship()],
      canvasData: { entities: 2, attributes: 2, relationships: 2 },
      currentQuery: 'match $x isa person; get;',
      currentKeyspace: 'gene',
      metaTypeInstances: {},
      graknSession: {
        transaction: () => Promise.resolve({ query: () => Promise.resolve({ collect: () => Promise.resolve([MockConcepts.getMockAnswer2()]) }),
          close: jest.fn(),
          getConcept: () => Promise.resolve([{ id: 1234 }]) }),
      },
      visStyle: { computeNodeStyle: jest.fn(), computeExplanationEdgeStyle: jest.fn() },
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

describe('actions', () => {
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
  //     // expect(actions[CANVAS_RESET].mock.calls).toHaveLength(1);
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

  test('RUN_CURRENT_QUERY', () => {
    store.dispatch(RUN_CURRENT_QUERY).then(() => {
      expect(validateQuery).toHaveBeenCalled();
      expect(loadingQuery.mock.calls).toHaveLength(2);
      expect(QuerySettings.getRolePlayersStatus).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.buildFromConceptMap).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(store.state.visFacade.fitGraphToWindow).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
    });
  });
});


describe('actions 2', () => {
  test('DELETE_SELECTED_NODES', () => {
    store.dispatch(DELETE_SELECTED_NODES, 'person').then(() => {
      expect(store.state.visFacade.deleteNode).toHaveBeenCalled();
      expect(store.state.selectedNodes).toBe(null);
    });
  });

  test('LOAD_ATTRIBUTES', () => {
    store.dispatch(LOAD_ATTRIBUTES, { visNode: mockVisNode, neighboursLimit: 1 }).then(() => {
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
      expect(QuerySettings.getRolePlayersStatus).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.buildFromConceptMap).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
      expect(loadingQuery.mock.calls).toHaveLength(1);
    });
  });
});

describe('actions 3', () => {
  test('EXPLAIN_CONCEPT', () => {
    store.dispatch(EXPLAIN_CONCEPT).then(() => {
      expect(mapAnswerToExplanationQuery).toHaveBeenCalled();
      expect(loadingQuery.mock.calls).toHaveLength(2);
      expect(VisualiserGraphBuilder.buildFromConceptMap).toHaveBeenCalled();
      // expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
      // expect(store.state.visFacade.updateNode).toHaveBeenCalled();
      // expect(store.state.visStyle.computeExplanationEdgeStyle).toHaveBeenCalled();
      // expect(store.state.visFacade.updateEdge).toHaveBeenCalled();
    });
  });
});
