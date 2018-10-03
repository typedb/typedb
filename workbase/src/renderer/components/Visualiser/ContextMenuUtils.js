import dataVisualiserStore from '@/components/Visualiser/store';

function verifyEnableExplain() {
  return (dataVisualiserStore.state.selectedNodes && dataVisualiserStore.state.selectedNodes[0].isInferred);
}

function verifyEnableDelete() {
  return (dataVisualiserStore.state.selectedNodes);
}

function verifyEnableShortestPath() {
  return (dataVisualiserStore.state.selectedNodes && dataVisualiserStore.state.selectedNodes.length === 2);
}

function repositionMenu(mouseEvent) {
  const contextMenu = document.getElementById('context-menu');
  contextMenu.style.left = `${mouseEvent.pointer.DOM.x}px`;
  contextMenu.style.top = `${mouseEvent.pointer.DOM.y}px`;
}

export default {
  verifyEnableExplain,
  verifyEnableDelete,
  verifyEnableShortestPath,
  repositionMenu,
};

