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

    /**
     * Return node colour based on its @baseType or default colour otherwise.
     * @param baseType
     * @returns {*}
     */
  getNodeColour(type, baseType) {
        // Meta-ontology
    if (type === '' && baseType !== API.GENERATED_RELATION_TYPE) {
      return {
        background: '#a80a74',
        highlight: {
          background: '#f15cc0',
        },
      };
    }

        // User defined ontology & instances
    switch (baseType) {
      case API.GENERATED_RELATION_TYPE:
        return {
          background: '#20a194',
          highlight: {
            background: '#0aca88',
          },
        };
      case API.RELATION_TYPE:
        return {
          background: '#20a194',
          highlight: {
            background: '#0aca88',
          },
        };
      case API.RESOURCE_TYPE:
        return {
          background: '#1d65cb',
          highlight: {
            background: '#0cb8f7',
          },
        };
      default:
        return this.node.colour;
    }
  }

    /**
     * Return node shape configuration based on its @baseType or default shape otherwise.
     * @param baseType
     * @returns {string}
     */
  getNodeShape(baseType) {
    switch (baseType) {
      case 'resource':
      case 'relation':
      case 'entity':
      default:
        return this.node.shape;
    }
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
  getEdgeColour() {
    return this.edge.colour;
  }

    /**
     * Return edge label font configuration.
     * @returns {{color: string}}
     */
  getEdgeFont() {
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
