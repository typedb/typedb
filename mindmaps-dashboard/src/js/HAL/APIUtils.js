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
    var baseTypeR = r[API.KEY_BASE_TYPE];
    var typeL = l[API.KEY_TYPE];
    var typeR = r[API.KEY_TYPE];
    var idL = l[API.KEY_ID];
    var idR = r[API.KEY_ID];

    if(idR === typeL || idR  === baseTypeL)
        return true;

    else if(idL === typeR || idL === baseTypeR)
        return false;

    else
        return (weight(baseTypeL) > weight(baseTypeR));
}

/**
 * Given a relationship between two hal resources and the default role name @roleName, what label should the relationship
 * be assigned?
 */
export function relationshipLabel(a, b, roleName) {
    var typeA = a[API.KEY_TYPE];
    var typeB = b[API.KEY_TYPE];

    var idA = a[API.KEY_ID];
    var idB = b[API.KEY_ID];

    if(idA === typeB || idB === typeA)
        return API.EDGE_LABEL_ISA;

    return roleName;
}

/**
 * Build a properties object for HalAPI.newResource() callback.
 */
export function resourceProperties(resource) {
    return {
        id: resource[API.KEY_ID],
        type: resource[API.KEY_TYPE],
        baseType: resource[API.KEY_BASE_TYPE],
        label: resource[API.KEY_ID]
    };
}

/*
 Internal functions
 */

/**
 * Calculate weight of a hal resource based on its type; used to establish directionality of a relationship between resources.
 */
function weight(baseType) {
    var weightMap = {
        "entity-type": 1,
        "relation-type": 2,
        "role-type": 3,
        "concept-type": 4
    };

    if (baseType in weightMap)
        return weightMap[baseType];
    else
        return 0;
}

