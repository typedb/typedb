/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */


import * as API from '../util/HALTerms';
import NodeSettings from '../NodeSettings';

/*
 * Styling options for visualised graph.
 */
export default class Style {
  constructor() {
    this.network = {
      background: '#383838',
    };

    this.node = {
      colour: {
        background: '#563891',
        highlight: {
          background: '#973fd8',
        },
      },
      shape: 'box',
    };

    this.edge = {
      colour: {
        color: '#00eca2',
        highlight: '#56fd92',
        hover: '#56fd92',
      },
      font: {
        color: '#00eca2',
        background: '#232323',
        strokeWidth: 0,
      },
    };
  }

  getNodeColour(type, baseType) {
    if (!Object.keys(NodeSettings.getNodeColour(type)).length && !Object.keys(NodeSettings.getNodeColour(baseType)).length) {
      return this.getDefaultNodeColour(type, baseType);
    } else if (type.length) {
      return NodeSettings.getNodeColour(type);
    }
    return NodeSettings.getNodeColour(baseType);
  }
    /**
     * Return node colour based on its @baseType or default colour otherwise.
     * @param baseType
     * @returns {*}
     */
  getDefaultNodeColour(type, baseType) {
    let colourObject;
        // User defined ontology & instances
    switch (baseType) {
      case API.GENERATED_RELATION_TYPE:
      case API.INFERRED_RELATION_TYPE:
        colourObject = {
          background: '#20a194',
          highlight: {
            background: '#0aca88',
          },
        };
        break;
      case API.ROLE_TYPE:
      case API.RELATION_TYPE:
      case API.RELATION:
        colourObject = {
          background: '#20a194',
          highlight: {
            background: '#0aca88',
          },
        };
        break;
      case API.RESOURCE_TYPE:
      case API.RESOURCE:
        colourObject = {
          background: '#1d65cb',
          highlight: {
            background: '#0cb8f7',
          },
        };
        break;
      default:
        if (type === '') {
          colourObject = {
            background: '#a80a74',
            highlight: {
              background: '#f15cc0',
            },
          };
        } else {
          colourObject = this.node.colour;
        }
    }

    return colourObject;
  }

    /**
     * Return node shape configuration based on its @baseType or default shape otherwise.
     * @param baseType
     * @returns {string}
     */
  getNodeShape(baseType) {
    let shape;
    switch (baseType) {
      case API.RELATION:
      case API.GENERATED_RELATION_TYPE:
      case API.INFERRED_RELATION_TYPE:
      case API.ROLE_TYPE:
        shape = 'dot';
        break;
      default:
        shape = this.node.shape;
    }
    return shape;
  }

  getNodeSize(baseType) {
    let size;
    switch (baseType) {
      case API.RELATION:
      case API.GENERATED_RELATION_TYPE:
      case API.INFERRED_RELATION_TYPE:
        size = 8;
        break;
      case API.ROLE_TYPE:
        size = 10;
        break;
      default:
        size = 25;
    }
    return size;
  }

    /**
     * Return node label font configuration based on its @baseType or default font otherwise.
     * @param baseType
     * @returns {{color: (string|string|string)}}
     */
  getNodeFont(type, baseType) {
    return {
      color: '#ffffff',
    };
  }

    /**
     * Return edge colour configuration.
     * @returns {ENGINE.Style.edge.colour|{color, highlight}}
     */
  getEdgeColour(label) {
    // let colour;
    // switch (label) {
    //   case 'relates':
    //     colour = '#2bbbad';
    //     break;
    //   default:
    //     colour = this.edge.colour;
    // }
    return this.edge.colour;
  }

    /**
     * Return edge label font configuration.
     * @returns {{color: string}}
     */
  getEdgeFont(label) {
    // let font;
    // switch (label) {
    //   case 'relates':
    //     font = '#2bbbad';
    //     break;
    //   default:
    //     font = this.edge.font;
    // }
    return this.edge.font;
  }

  clusterColour() {
    return {
      background: this.node.colour.background,
      highlight: {
        background: this.node.colour.background,
      },
    };
  }

  clusterFont() {
    return {
      color: '#ffffff',
    };
  }
}
