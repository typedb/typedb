

const DEFAULT_NODE_SHAPE = 'box';
const DEFAULT_NODE_SIZE = 25;
const DEFAULT_NODE_BACKGROUND = '#563891';
const DEFAULT_NODE_HIGHLIGHT = '#973fd8';
const DEFAULT_NODE_DIMMED = 'rgba(86, 56, 145, 0.2)';


function nodeColour(node) {
  let backgroundCol;
  let highlightCol;
  let dimmedCol;

  switch (node.baseType) {
    case 'INFERRED_RELATIONSHIP_TYPE':
      backgroundCol = '#20a194';
      highlightCol = '#0aca88';
      dimmedCol = 'rgba(32, 161, 148, 0.5)';
      break;
    case 'ROLE':
      backgroundCol = '#20a194';
      highlightCol = '#56fd92';
      dimmedCol = 'rgba(32, 161, 148, 0.6)';
      break;
    case 'RELATIONSHIP_TYPE':
    case 'RELATIONSHIP':
      backgroundCol = '#20a194';
      highlightCol = '#0aca88';
      dimmedCol = 'rgba(32, 161, 148, 0.6)';
      break;
    case 'ATTRIBUTE_TYPE':
    case 'ATTRIBUTE':
      backgroundCol = '#1d65cb';
      highlightCol = '#0cb8f7';
      dimmedCol = 'rgba(29, 101, 203, 0.5)';
      break;
    default:
      if (node.type === '') {
        backgroundCol = '#a80a74';
        highlightCol = '#f15cc0';
        dimmedCol = 'rgba(168, 10, 116, 0.5)';
      } else {
        backgroundCol = DEFAULT_NODE_BACKGROUND;
        highlightCol = DEFAULT_NODE_HIGHLIGHT;
        dimmedCol = DEFAULT_NODE_DIMMED;
      }
  }

  return {
    background: backgroundCol,
    border: backgroundCol,
    dimmedColor: dimmedCol,
    highlight: { background: highlightCol, border: highlightCol },
    hover: { background: highlightCol, border: highlightCol },
  };
}


function nodeFont() {
  return {
    color: '#ffffff',
    dimmedColor: 'rgba(32, 161, 148, 0.5)',
    face: 'Lekton',
  };
}

function nodeShape(node) {
  let shape;
  switch (node.baseType) {
    case 'RELATIONSHIP':
    case 'ROLE':
      shape = 'dot';
      break;
    case 'RELATIONSHIP_TYPE':
      shape = 'diamond';
      break;
    case 'ENTITY_TYPE':
      shape = 'box';
      break;
    case 'ATTRIBUTE_TYPE':
      shape = 'ellipse';
      break;
    default:
      shape = DEFAULT_NODE_SHAPE;
  }
  return shape;
}

function nodeSize(node) {
  let size;
  switch (node.baseType) {
    case 'RELATIONSHIP':
    case 'INFERRED_RELATIONSHIP_TYPE':
      size = 8;
      break;
    case 'ROLE':
      size = 10;
      break;
    default:
      size = DEFAULT_NODE_SIZE;
  }
  return size;
}

function computeNodeStyle(node) {
  return {
    color: nodeColour(node),
    colorClone: nodeColour(node),
    font: nodeFont(node),
    fontClone: nodeFont(node),
    shape: nodeShape(node),
    size: nodeSize(node),
  };
}

function computeEdgeStyle() {
  return {
    font: {
      color: '#00eca2',
      background: '#1B1B1B',
      strokeWidth: 0,
    },
    color: {
      color: 'rgba(0, 236, 162, 1)',
      highlight: '#56fd92',
      hover: '#56fd92',
    },
    hoverWidth: 2,
    selectionWidth: 2,
    arrowStrikethrough: false,
    arrows: { to: { enabled: false } },
    smooth: {
      enabled: false,
      forceDirection: 'none',
    },
  };
}

export default {
  computeNodeStyle,
  computeEdgeStyle,
};
