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
import VisualiserGraphBuilder from '@/components/Visualiser/VisualiserGraphBuilder';
import actions from '@/components/Visualiser/store/actions';
import mutations from '@/components/Visualiser/store/mutations';
import getters from '@/components/Visualiser/store/getters';
import {
  addResetGraphListener,
  loadMetaTypeInstances,
  getNeighboursData,
  computeAttributes,
  validateQuery,
  mapAnswerToExplanationQuery,
} from '@/components/Visualiser/VisualiserUtils';
import VisualiserCanvasEventsHandler from '@/components/Visualiser/VisualiserCanvasEventsHandler';
import QuerySettings from '@/components/Visualiser/RightBar/SettingsTab/QuerySettings';
import MockConcepts from '../../../../helpers/MockConcepts';

jest.mock('@/components/Visualiser/VisualiserGraphBuilder', () => ({
  prepareNodes: jest.fn(),
  buildFromConceptMap: jest.fn().mockImplementation(() => Promise.resolve({ nodes: [MockConcepts.getMockEntity1()], edges: [{ from: 1234, to: 4321, label: 'son' }] })),
}));

jest.mock('grakn', () => ({ txType: { WRITE: 'write' } }));

jest.mock('@/../Logger', () => ({ error: () => {} }));

jest.mock('@/components/shared/PersistentStorage', () => {});

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

jest.mock('@/components/Visualiser/RightBar/SettingsTab/QuerySettings', () => ({
  getRolePlayersStatus: jest.fn(),
}));

Vue.use(Vuex);

describe('actions', () => {
  const graknSession = {
    transaction: () => Promise.resolve({ query: () => Promise.resolve({ collect: () => Promise.resolve([MockConcepts.getMockAnswer2()]) }),
      close: jest.fn(),
      getConcept: () => Promise.resolve([{ id: 1234 }]) }),
  };

  test('INITIALISE_VISUALISER', () => {
    const mockContainer = {};
    const setVisFacade = jest.fn();
    const store = new Vuex.Store({
      actions,
      mutations: { setVisFacade },
    });
    const mockVisFacade = { initVisualiser: () => ({ registerEventHandler: () => { } }) };
    store.dispatch(INITIALISE_VISUALISER, { container: mockContainer, visFacade: mockVisFacade }).then(() => {
      expect(addResetGraphListener).toHaveBeenCalled();
      expect(setVisFacade.mock.calls).toHaveLength(1);
      expect(VisualiserCanvasEventsHandler.registerHandlers).toHaveBeenCalled();
    });
  });

  test('CANVAS_RESET', () => {
    const store = new Vuex.Store({
      state: {
        visFacade: { resetCanvas: jest.fn(), getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        selectedNodes: [MockConcepts.getMockRelationship()],
        canvasData: { entities: 2, attributes: 2, relationships: 2 },
      },
      actions,
      mutations,
    });
    store.dispatch(CANVAS_RESET).then(() => {
      expect(store.state.visFacade.resetCanvas).toHaveBeenCalled();
      expect(store.state.selectedNodes).toBe(null);
      expect(store.state.canvasData.entities).toBe(0);
      expect(store.state.canvasData.attributes).toBe(0);
      expect(store.state.canvasData.relationships).toBe(0);
    });
  });

  test('CURRENT_KEYSPACE_CHANGED', () => {
    const canvasReset = jest.fn();
    const updateMetatypeInstances = jest.fn();

    const store = new Vuex.Store({
      state: {
        currentQuery: 'match $x isa person; get;',
        currentKeyspace: 'gene',
        grakn: { session: () => jest.fn() },
      },
      actions: {
        [CURRENT_KEYSPACE_CHANGED]: actions[CURRENT_KEYSPACE_CHANGED],
        [CANVAS_RESET]: canvasReset,
        [UPDATE_METATYPE_INSTANCES]: updateMetatypeInstances,
      },
      mutations,
    });

    store.dispatch(CURRENT_KEYSPACE_CHANGED, 'pokemon').then(() => {
      expect(canvasReset).toHaveBeenCalled();
      expect(store.state.currentQuery).toBe('');
      expect(store.state.currentKeyspace).toBe('pokemon');
      expect(updateMetatypeInstances).toHaveBeenCalled();
    });
  });

  test('UPDATE_METATYPE_INSTANCES', () => {
    const metaTypeInstances = jest.fn();
    const store = new Vuex.Store({
      state: {
        graknSession,
      },
      actions,
      mutations: { metaTypeInstances },
    });

    store.dispatch(UPDATE_METATYPE_INSTANCES).then(() => {
      expect(loadMetaTypeInstances).toHaveBeenCalled();
      expect(metaTypeInstances.mock.calls).toHaveLength(1);
    });
  });

  test('OPEN_GRAKN_TX', () => {
    const store = new Vuex.Store({
      state: {
        graknSession: {
          transaction: jest.fn(),
        },
      },
      actions,
    });

    store.dispatch(OPEN_GRAKN_TX).then(() => {
      expect(store.state.graknSession.transaction).toHaveBeenCalled();
    });
  });

  test('UPDATE_NODES_LABEL', () => {
    const store = new Vuex.Store({
      state: {
        visFacade: {
          updateNode: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        graknSession,
      },
      actions,
    });

    store.dispatch(UPDATE_NODES_LABEL, 'person').then(() => {
      expect(store.state.visFacade.getAllNodes).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.prepareNodes).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
    });
  });

  test('UPDATE_NODES_COLOUR', () => {
    const store = new Vuex.Store({
      state: {
        visFacade: {
          updateNode: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        graknSession,
        visStyle: { computeNodeStyle: jest.fn() },
      },
      actions,
    });

    store.dispatch(UPDATE_NODES_COLOUR, 'person').then(() => {
      expect(store.state.visFacade.getAllNodes).toHaveBeenCalled();
      expect(store.state.visStyle.computeNodeStyle).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
    });
  });

  test('LOAD_NEIGHBOURS', () => {
    const loadingQuery = jest.fn();
    const mockVisNode = MockConcepts.getMockEntity1();

    const store = new Vuex.Store({
      state: {
        visFacade: {
          updateNode: jest.fn(),
          addToCanvas: jest.fn(),
          fitGraphToWindow: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        graknSession,
      },
      actions,
      mutations: { loadingQuery, updateCanvasData: mutations.updateCanvasData },
    });

    store.dispatch(LOAD_NEIGHBOURS, { visNode: mockVisNode, neighboursLimit: 1 }).then(() => {
      expect(loadingQuery.mock.calls).toHaveLength(2);
      expect(getNeighboursData).toHaveBeenCalled();
      expect(mockVisNode.offset).toBe(1);
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(store.state.visFacade.fitGraphToWindow).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.prepareNodes).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
    });
  });

  test('RUN_CURRENT_QUERY', () => {
    const loadingQuery = jest.fn();

    const store = new Vuex.Store({
      state: {
        visFacade: {
          updateNode: jest.fn(),
          addToCanvas: jest.fn(),
          fitGraphToWindow: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        graknSession,
      },
      actions,
      mutations: { loadingQuery, updateCanvasData: mutations.updateCanvasData },
    });

    store.dispatch(RUN_CURRENT_QUERY).then(() => {
      expect(validateQuery).toHaveBeenCalled();
      // expect(loadingQuery.mock.calls).toHaveLength(2);
      expect(QuerySettings.getRolePlayersStatus).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.buildFromConceptMap).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(store.state.visFacade.fitGraphToWindow).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
    });
  });

  test('DELETE_SELECTED_NODES', () => {
    const store = new Vuex.Store({
      state: {
        visFacade: {
          deleteNode: jest.fn(),
        },
        selectedNodes: [MockConcepts.getMockRelationship()],
      },
      actions,
      mutations: { selectedNodes: mutations.selectedNodes },
    });

    store.dispatch(DELETE_SELECTED_NODES, 'person').then(() => {
      expect(store.state.visFacade.deleteNode).toHaveBeenCalled();
      expect(store.state.selectedNodes).toBe(null);
    });
  });

  test('LOAD_ATTRIBUTES', () => {
    const loadingQuery = jest.fn();
    const mockVisNode = MockConcepts.getMockEntity1();

    const store = new Vuex.Store({
      state: {
        visFacade: {
          updateNode: jest.fn(),
          addToCanvas: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        graknSession,
      },
      actions,
      mutations: { loadingQuery, updateCanvasData: mutations.updateCanvasData },
    });

    store.dispatch(LOAD_ATTRIBUTES, { visNode: mockVisNode, neighboursLimit: 1 }).then(() => {
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
      expect(QuerySettings.getRolePlayersStatus).toHaveBeenCalled();
      expect(VisualiserGraphBuilder.buildFromConceptMap).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
      expect(loadingQuery.mock.calls).toHaveLength(1);
    });
  });

  test('EXPLAIN_CONCEPT', () => {
    const loadingQuery = jest.fn();
    const store = new Vuex.Store({
      state: {
        visFacade: {
          updateNode: jest.fn(),
          updateEdge: jest.fn(),
          addToCanvas: jest.fn(),
          getAllNodes: jest.fn().mockImplementation(() => [{ id: 1234, type: 'person' }]),
        },
        selectedNodes: [MockConcepts.getMockRelationship()],
        graknSession,
        visStyle: { computeExplanationEdgeStyle: jest.fn() },
      },
      actions,
      mutations: { loadingQuery, updateCanvasData: mutations.updateCanvasData },
      getters: { selectedNode: getters.selectedNode },
    });

    store.dispatch(EXPLAIN_CONCEPT).then(async () => {
      await new Promise(r => setTimeout(r, 1000));

      expect(mapAnswerToExplanationQuery).toHaveBeenCalled();
      expect(loadingQuery.mock.calls).toHaveLength(2);
      expect(VisualiserGraphBuilder.buildFromConceptMap).toHaveBeenCalled();
      expect(store.state.visFacade.addToCanvas).toHaveBeenCalled();
      expect(computeAttributes).toHaveBeenCalled();
      expect(store.state.visFacade.updateNode).toHaveBeenCalled();
      expect(store.state.visStyle.computeExplanationEdgeStyle).toHaveBeenCalled();
      expect(store.state.visFacade.updateEdge).toHaveBeenCalled();
    });
  });
});

