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
import User from './User';


// Default constant values
const NODE_LABELS_KEY = 'node_labels';
const NODE_COLOURS_KEY = 'node_colours';


export default {

  getLabelProperties(type) {
    const currentKeyspace = User.getCurrentKeySpace();
    const nodesLabels = localStorage.getItem(NODE_LABELS_KEY);

    if (nodesLabels === null) {
      localStorage.setItem(NODE_LABELS_KEY, JSON.stringify({ [currentKeyspace]: { [type]: [] } }));
      return [];
    }
    const nodesLabelsObject = JSON.parse(nodesLabels);
    if (!(currentKeyspace in nodesLabelsObject) || !(type in nodesLabelsObject[currentKeyspace])) {
      return [];
    }

    return nodesLabelsObject[currentKeyspace][type];
  },
  removeTypeLabel(type, label) {
    const filtered = this.getLabelProperties(type).filter(x => x !== label);
    this.setTypeLabels(type, filtered);
  },

  addTypeLabel(type, label) {
    const nodesLabels = this.getLabelProperties(type);
    nodesLabels.push(label);

    this.setTypeLabels(type, nodesLabels);
  },

  setTypeLabels(type, nodesLabelsParam) {
    const nodesLabels = JSON.parse(localStorage.getItem(NODE_LABELS_KEY));
    const currentKeyspace = User.getCurrentKeySpace();

    if (!(currentKeyspace in nodesLabels)) {
      Object.assign(nodesLabels, { [currentKeyspace]: {} });
    }
    Object.assign(nodesLabels[currentKeyspace], {
      [type]: nodesLabelsParam,
    });
    localStorage.setItem(NODE_LABELS_KEY, JSON.stringify(nodesLabels));
  },

  // Nodes colours

  // if no colours are set for a given type it will return an {}

  getNodeColour(type) {
    if (type !== undefined && type.length) {
      const currentKeyspace = User.getCurrentKeySpace();
      const nodesColours = localStorage.getItem(NODE_COLOURS_KEY);

      if (nodesColours === null) {
        localStorage.setItem(NODE_COLOURS_KEY, JSON.stringify({ [currentKeyspace]: { [type]: {} } }));
        return {};
      }
      const nodeColoursObject = JSON.parse(nodesColours);
      if (!(currentKeyspace in nodeColoursObject) || !(type in nodeColoursObject[currentKeyspace])) {
        return {};
      }

      return nodeColoursObject[currentKeyspace][type];
    }

    return {};
  },


// pass a {} to remove colours from given type
  setNodeColour(type, coloursObj) {
    const nodesColours = JSON.parse(localStorage.getItem(NODE_COLOURS_KEY));
    const currentKeyspace = User.getCurrentKeySpace();

    if (!(currentKeyspace in nodesColours)) {
      Object.assign(nodesColours, { [currentKeyspace]: {} });
    }

    Object.assign(nodesColours[currentKeyspace], {
      [type]: coloursObj,
    });
    localStorage.setItem(NODE_COLOURS_KEY, JSON.stringify(nodesColours));
  },
};
