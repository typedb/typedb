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

/*
 * Various miscellaneous functions used by the HALParser class.
 */

 /*
  * Build label to show in the visualiser, based on the node type.
  */
function buildLabel(resource) {
  let label = resource[API.KEY_TYPE];

  switch (resource[API.KEY_BASE_TYPE]) {
    case API.ENTITY_TYPE:
    case API.ENTITY:
      label = `${resource[API.KEY_TYPE]}: ${resource[API.KEY_ID]}`;
      break;
    case API.RELATION_TYPE:
    case API.RELATION:
      label = `${resource[API.KEY_BASE_TYPE].substring(0, 3)}: ${resource[API.KEY_TYPE]}`;
      break;
    case API.RESOURCE_TYPE:
    case API.RESOURCE:
      label = resource[API.KEY_VALUE];
      break;
    case API.GENERATED_RELATION_TYPE:
      label = resource[API.KEY_TYPE] || '';
      break;

    default:
      label = resource[API.KEY_TYPE];
  }

  if (API.KEY_VALUE in resource) { label = resource[API.KEY_VALUE] || label; }
  if (API.KEY_NAME in resource) { label = resource[API.KEY_NAME] || label; }


  return label;
}

/**
 * Used to decide the directionality of a relationship between two resources,
 * based on the API.KEY_DIRECTION property.
 */
export function edgeLeftToRight(a, b) {
  if (API.KEY_DIRECTION in b) {
    if (b[API.KEY_DIRECTION] === 'OUT') { return false; }
  }

  return true;
}