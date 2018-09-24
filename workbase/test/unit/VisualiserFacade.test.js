
import Facade from '@/components/CanvasVisualiser/Facade';

jest.mock('@/components/CanvasVisualiser/Visualiser', () => ({
  createVisualiser: () => ({
    render: jest.fn(),
    edgesConnectedToNode: jest.fn(),
    getEdge: jest.fn(),
    deleteEdge: jest.fn(),
    deleteNode: jest.fn(),
    getNode: jest.fn(),
    clearGraph: jest.fn(),
  }),
}));

let facade;

beforeEach(() => {
  const style = {
    computeNodeStyle: jest.fn(),
    computeEdgeStyle: jest.fn(),
  };
  facade = Facade.initVisualiser({}, style);
});

describe('Visualiser Facade methods', () => {
  test('deleteEdgesOnNode', () => {
    facade.container.visualiser.edgesConnectedToNode.mockImplementation(() => (['098', '765']));
    facade.deleteEdgesOnNode('234');
    expect(facade.container.visualiser.deleteEdge).toHaveBeenNthCalledWith(1, '098');
    expect(facade.container.visualiser.deleteEdge).toHaveBeenNthCalledWith(2, '765');
  });

  test('deleteEdgesOnNode with label', () => {
    facade.container.visualiser.edgesConnectedToNode.mockImplementation(() => (['098', '765']));
    facade.container.visualiser.getEdge.mockImplementation(() => ([{ id: '098', label: 'deleteme' }, { id: '765', label: 'exists' }]));
    facade.deleteEdgesOnNode('234', 'deleteme');
    expect(facade.container.visualiser.deleteEdge).toHaveBeenNthCalledWith(1, '098');
    expect(facade.container.visualiser.deleteEdge).toHaveBeenCalledTimes(1);
  });

  test('deleteFromCanvas', () => {
    facade.container.visualiser.edgesConnectedToNode.mockImplementation(() => ([]));
    facade.deleteFromCanvas(['234', '567']);
    expect(facade.container.visualiser.deleteNode).toHaveBeenNthCalledWith(1, '234');
    expect(facade.container.visualiser.edgesConnectedToNode).toHaveBeenNthCalledWith(1, '234');
    expect(facade.container.visualiser.deleteNode).toHaveBeenNthCalledWith(2, '567');
    expect(facade.container.visualiser.edgesConnectedToNode).toHaveBeenNthCalledWith(2, '567');
  });

  // TODO
  test('addToCanvas', () => {});
  test('getAllNode', () => {});
  test('resetCanvas', () => {});
  test('registerEventHandler', () => {});
  test('fitGraphToWindow', () => {});
});
