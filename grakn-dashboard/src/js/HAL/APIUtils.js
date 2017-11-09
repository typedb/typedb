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

import * as API from '../util/HALTerms';

/*
 * Various miscellaneous functions used by the HALParser class.
 */

 /*
  * Build label to show in the visualiser, based on the node type.
  */
function buildLabel(attribute) {
  let label = attribute[API.KEY_TYPE];

  switch (attribute[API.KEY_BASE_TYPE]) {
    case API.ENTITY_TYPE:
    case API.ENTITY:
      label = `${attribute[API.KEY_TYPE]}: ${attribute[API.KEY_ID]}`;
      break;
    case API.RELATIONSHIP_TYPE:
    case API.RELATIONSHIP:
      label = `${attribute[API.KEY_BASE_TYPE].substring(0, 3)}: ${attribute[API.KEY_TYPE]}`;
      break;
    case API.ATTRIBUTE_TYPE:
    case API.ATTRIBUTE:
      label = attribute[API.KEY_VALUE];
      break;

    default:
      label = attribute[API.KEY_TYPE];
  }

  if (API.KEY_VALUE in attribute) { label = attribute[API.KEY_VALUE] || label; }
  if (API.KEY_NAME in attribute) { label = attribute[API.KEY_NAME] || label; }


  return label;
}

/**
 * Used to decide the directionality of a relationshipship between two attributes,
 * based on the API.KEY_DIRECTION property.
 */
export function edgeLeftToRight(a:Object, b:Object) {
  if (API.KEY_DIRECTION in b) {
    if (b[API.KEY_DIRECTION] === 'OUT') { return false; }
  }

  return true;
}

/**
 * Extract default properties from HAL object, mainly properties from the HAL's state
 * @param {Object} attribute HAL Object
 * @returns {Object} Object containing all the attributes embedded in a HAL object
 * @public
 */
export function defaultProperties(attribute:Object) {
  return {
    id: attribute[API.KEY_ID],
    href: attribute[API.KEY_LINKS][API.KEY_SELF][API.KEY_HREF],
    type: attribute[API.KEY_TYPE] || '',
    baseType: attribute[API.KEY_BASE_TYPE],
    label: buildLabel(attribute),
    explore: attribute[API.KEY_LINKS][API.KEY_EXPLORE][0][API.KEY_HREF],
    implicit: attribute[API.KEY_IMPLICIT] || false,
  };
}


/**
 * Extract from "_embedded" all the nodes that are "attribute-type" and
 * build a new object that lists all the attributes related to a node.
 * Structure: e.g. {"title":"Skyfall","duration":"263"}
 * @param {Object} attribute HAL object
 * @returns {Object} Object containing all the attributes embedded in a HAL object
 * @public
 */
export function extractAttributes(attribute:Object) {
  if (!(API.KEY_EMBEDDED in attribute)) return {};

  const embeddedObject = attribute[API.KEY_EMBEDDED];
  return Object.keys(embeddedObject).reduce((newAttributesObject, key) => {
      // TODO: decide if we want to support multiple values as label of a visualiser node. For now we pick the first value.
    const currentAttribute = embeddedObject[key][0];
    if (currentAttribute[API.KEY_BASE_TYPE] === API.ATTRIBUTE_TYPE || currentAttribute[API.KEY_BASE_TYPE] === API.ATTRIBUTE) {
      return Object.assign({}, newAttributesObject, { [key]:
      {
        id: currentAttribute[API.KEY_ID],
        label: buildLabel(currentAttribute),
        link: currentAttribute[API.KEY_LINKS][API.KEY_SELF][API.KEY_HREF],
      } });
    }
    return newAttributesObject;
  }, {});
}

/**
 *Work in progress to generate links, this will need to change to use AJAX requests.
 */
export function nodeLinks(attribute:Object) {
  const linksObject = attribute[API.KEY_LINKS];
  return Object.keys(linksObject)
        .filter(x => (x !== API.KEY_SELF && x !== API.KEY_EXPLORE))
        .reduce((newLinksObject, key) => Object.assign({}, newLinksObject, { [key]: linksObject[key].length }), {});
}
