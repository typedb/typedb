/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

import * as API from './APITerms';

/*
 * Various miscellaneous functions used by the HALParser class.
 */

/**
 * Used to infer directionality of a relationship between two hal resources: @l and @r.
 * The hal resources that is deemed more `significant` will have the relationship directed at it.
 */
export function leftSignificant(l, r) {
    var baseTypeL = l[API.KEY_BASE_TYPE];
    var typeL = l[API.KEY_TYPE];
    var idR = r[API.KEY_ID];

    if(idR === typeL || idR  === baseTypeL)
        return true;
    else
        return false;
}

/**
 * Build a properties object for HalAPI.newResource() callback.
 */
export function resourceProperties(resource) {
    return {
        id: resource[API.KEY_ID],
        type: resource[API.KEY_TYPE],
        baseType: resource[API.KEY_BASE_TYPE],
        label: buildLabel(resource),
        ontology: resource[API.KEY_LINKS][API.KEY_ONTOLOGY][0][API.KEY_HREF]
    };
}

/*
 Internal functions
 */
function buildLabel(resource) {
    var label = resource[API.KEY_ID];

    if(API.KEY_VALUE in resource)
        label = resource[API.KEY_VALUE] || label;

    if(resource[API.KEY_BASE_TYPE] === API.ENTITY_TYPE)
        label = resource[API.KEY_TYPE] + ": " + label;

    return label;
}
