
import NodeSettings from './RightBar/SettingsTab/DisplaySettings';


const DEFAULT_NODE_SHAPE = 'box';
const DEFAULT_NODE_SIZE = 25;
const DEFAULT_NODE_BACKGROUND = '#563891';
const DEFAULT_NODE_HIGHLIGHT = '#973fd8';
const DEFAULT_NODE_DIMMED = 'rgba(86, 56, 145, 0.2)';

function lightenDarkenColor(col, amt) {
  let usePound = false;

  if (col[0] === '#') {
    col = col.slice(1);
    usePound = true;
  }

  const num = parseInt(col, 16);

  let r = (num >> 16) + amt; // eslint-disable-line no-bitwise

  if (r > 255) r = 255;
  else if (r < 0) r = 0;

  let b = ((num >> 8) & 0x00FF) + amt; // eslint-disable-line no-bitwise

  if (b > 255) b = 255;
  else if (b < 0) b = 0;

  let g = (num & 0x0000FF) + amt; // eslint-disable-line no-bitwise

  if (g > 255) g = 255;
  else if (g < 0) g = 0;

  return (usePound ? '#' : '') + (g | (b << 8) | (r << 16)).toString(16);// eslint-disable-line no-bitwise
}

function convertHex(colour, opacity) {
  const hex = colour.replace('#', '');
  const r = parseInt(hex.substring(0, 2), 16);
  const g = parseInt(hex.substring(2, 4), 16);
  const b = parseInt(hex.substring(4, 6), 16);

  const result = `rgba(${r},${g},${b},${opacity})`;
  return result;
}

function colourFromStorage(colour) {
  const backgroundCol = colour;
  const highlightCol = lightenDarkenColor(colour, 30);
  const dimmedCol = convertHex(colour, 0.2);

  return {
    background: backgroundCol,
    border: backgroundCol,
    dimmedColor: dimmedCol,
    highlight: { background: highlightCol, border: highlightCol },
    hover: { background: highlightCol, border: highlightCol },
  };
}

function nodeColour(node) {
  const colour = NodeSettings.getTypeColours(node.type);
  if (colour.length) return colourFromStorage(colour);


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
    size: 15,
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
      shape = 'box';
      break;
    case 'ENTITY_TYPE':
      shape = 'box';
      break;
    case 'ATTRIBUTE_TYPE':
      shape = 'box';
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
    shadow: {
      enabled: true,
      size: 10,
      x: 2,
      y: 2,
    },
  };
}

function computeExplanationEdgeStyle() {
  return {
    color: {
      color: 'rgba(151, 63, 216, 1)',
      highlight: '#973fd8',
      hover: '#973fd8',
    },
    dashes: true,
  };
}

function computeShortestPathEdgeStyle() {
  return {
    color: {
      color: 'rgba(166, 226, 46, 1)',
      highlight: 'rgba(166, 226, 46, 1)',
      hover: 'rgba(166, 226, 46, 1)',
    },
    dashes: true,
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
  computeExplanationEdgeStyle,
  computeShortestPathEdgeStyle,
};
