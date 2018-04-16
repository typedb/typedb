/*-
 * #%L
 * grakn-dashboard
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
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

/* @flow */


export const RELATIONSHIP_TYPE = 'RELATIONSHIP_TYPE';
export const RELATIONSHIP = 'RELATIONSHIP';

export const TYPE_TYPE = 'TYPE';

export const RULE_TYPE = 'RULE_TYPE';
export const RULE = 'RULE';


export const ATTRIBUTE = 'ATTRIBUTE';
export const ATTRIBUTE_TYPE = 'ATTRIBUTE_TYPE';

export const ROLE_TYPE = 'ROLE_TYPE';
export const ROLE = 'ROLE';

export const ENTITY_TYPE = 'ENTITY_TYPE';
export const ENTITY = 'ENTITY';

export const INFERRED_RELATIONSHIP_TYPE = 'INFERRED_RELATIONSHIP';


export const ROOT_CONCEPT = 'thing';

export const KEY_ID = 'id';
export const KEY_TYPE = 'type';
export const KEY_BASE_TYPE = 'base-type';
export const KEY_VALUE = 'value';
export const KEY_LABEL = 'label';
export const KEY_IMPLICIT = 'implicit';
export const KEY_INFERRED = 'inferred';
export const KEY_ABSTRACT = 'abstract';
export const KEY_DATATYPE = 'data-type';
export const AT_ID = '@id';
export const KEY_EXPLANATION_QUERY = 'explanation-query';

/*
  * Build label to show in the visualiser, based on the node type.
  */
function buildLabel(nodeObject) {
  // Only meta types have label field in properties
  if (KEY_LABEL in nodeObject) return nodeObject[KEY_LABEL];

  // If it's an instances
  let label;
  switch (nodeObject[KEY_BASE_TYPE]) {
    case ENTITY:
      label = `${nodeObject[KEY_TYPE].label}: ${nodeObject[KEY_ID]}`;
      break;
    case RELATIONSHIP:
      label = '';
      break;
    case ATTRIBUTE:
      label = `${nodeObject[KEY_TYPE].label}: ${nodeObject[KEY_VALUE]}`;
      break;

    default:
      label = nodeObject[KEY_TYPE];
  }

  return label;
}

/**
 * Extract properties that reside on the concept node
 * @param {Object} attribute Engine response Object
 * @returns {Object} Object containing all the main properties of the response Object
 * @public
 */
export function conceptProperties(object:Object) {
  return {
    id: object[KEY_ID],
    href: object[AT_ID],
    type: (object[KEY_TYPE]) ? object[KEY_TYPE].label : '',
    baseType: (!object.inferred) ? object[KEY_BASE_TYPE] : `INFERRED_${object[KEY_BASE_TYPE]}`,
    label: buildLabel(object),
    implicit: object[KEY_IMPLICIT] || false,
    inferred: object[KEY_INFERRED] || false,
    abstract: object[KEY_ABSTRACT],
    explanationQuery: object[KEY_EXPLANATION_QUERY] || '',
  };
}

export function parseAttributes(attributes:Object[]) {
  return attributes.map(attr => ({
    id: attr[KEY_ID],
    href: attr[AT_ID],
    value: attr[KEY_VALUE],
    dataType: attr[KEY_DATATYPE],
    type: (attr[KEY_TYPE]) ? attr[KEY_TYPE].label : attr[KEY_LABEL],
  }))
    .reduce((array, current) => array.concat(current), []);
}

