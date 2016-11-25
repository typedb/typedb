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

import * as API from './APITerms';

/*
 * Various miscellaneous functions used by the HALParser class.
 */

/**
 * Used to decide the directionality of a relationship between two resources, based on the API.KEY_DIRECTION property.
 */
export function edgeLeftToRight(a, b) {
    if (API.KEY_DIRECTION in b)
        if (b[API.KEY_DIRECTION] === "OUT")
            return false;

    return true;
}

/**
 * Build a properties object for HalAPI.newResource() callback.
 */
export function defaultProperties(resource) {
    return {
        id: resource[API.KEY_ID],
        type: resource[API.KEY_TYPE],
        baseType: resource[API.KEY_BASE_TYPE],
        label: buildLabel(resource),
        ontology: resource[API.KEY_LINKS][API.KEY_ONTOLOGY][0][API.KEY_HREF]
    };
}

export function extractResources(resource) {
    if (API.KEY_EMBEDDED in resource) {
        var embeddedObject = resource[API.KEY_EMBEDDED];
        return Object.keys(embeddedObject).reduce((newResourcesObject, key) => {
            var currentResourceList = embeddedObject[key];
            currentResourceList.forEach(function(currentResource) {
                if (currentResource[API.KEY_BASE_TYPE] === API.RESOURCE_TYPE)
                    newResourcesObject[key] = {
                        id: currentResource[API.KEY_ID],
                        label: buildLabel(currentResource),
                        link: currentResource[API.KEY_LINKS][API.KEY_SELF][API.KEY_HREF]
                    };
            });
            return newResourcesObject;
        }, {});
    }
}

export function nodeLinks(resource) {
    var linksObject = resource[API.KEY_LINKS];
    return Object.keys(linksObject)
        .filter(x => (x !== API.KEY_SELF && x !== API.KEY_ONTOLOGY))
        .reduce((newLinksObject, key) => {
            newLinksObject[key] = linksObject[key].length;
            return newLinksObject
        }, {});
}

/*
 Internal functions
 */
function buildLabel(resource) {
    var label = undefined;

    switch (resource[API.KEY_BASE_TYPE]) {
        case API.ENTITY_TYPE:
            label = resource[API.KEY_TYPE] + ": " + resource[API.KEY_ID];
            break;
        case API.RELATION_TYPE:
            label = resource[API.KEY_BASE_TYPE].substring(0,3) + ": " + resource[API.KEY_TYPE];
            break;
        case API.RESOURCE_TYPE:
            label = resource[API.KEY_VALUE];
            break;
        case API.GENERATED_RELATION_TYPE:
            label = resource[API.KEY_TYPE] || "";
            break;

        default:
            label = resource[API.KEY_ID];
    }

    if (API.KEY_VALUE in resource)
        label = resource[API.KEY_VALUE] || label;

    return label;
}
