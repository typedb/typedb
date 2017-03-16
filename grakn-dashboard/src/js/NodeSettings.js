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


export default {

  getLabelProperties(type) {
    const currentKeyspace = User.getCurrentKeySpace();
    const nodesLabels = localStorage.getItem(NODE_LABELS_KEY);

    if (nodesLabels === null) {
      localStorage.setItem(NODE_LABELS_KEY, JSON.stringify({ [currentKeyspace]: { [type]: [] } }));
      return [];
    }
    const nodesLabelsObject = JSON.parse(nodesLabels);
    if (!(currentKeyspace in nodesLabelsObject)) {
      return [];
    }

    if (!(type in nodesLabelsObject[currentKeyspace])) {
      Object.assign(nodesLabelsObject[currentKeyspace], {
        [type]: [],
      });
      return [];
    }

    return nodesLabelsObject[currentKeyspace][type];
  },

  setTypeLabel(type, labels) {
    const nodesLabels = this.getLabelProperties(type);
    labels.forEach((label) => {
      nodesLabels.push(label);
    });
    this.setNodesLabels(type, nodesLabels);
  },

  setNodesLabels(type, nodesLabelsParam) {
    const nodesLabels = JSON.parse(localStorage.getItem(NODE_LABELS_KEY));
    Object.assign(nodesLabels[User.getCurrentKeySpace()], {
      [type]: nodesLabelsParam,
    });
    localStorage.setItem(NODE_LABELS_KEY, JSON.stringify(nodesLabels));
  },
};
