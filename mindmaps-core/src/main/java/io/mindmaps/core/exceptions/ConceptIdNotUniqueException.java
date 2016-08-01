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

package io.mindmaps.core.exceptions;

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.core.model.Concept;

public class ConceptIdNotUniqueException extends ConceptException {
    public ConceptIdNotUniqueException(Concept concept, DataType.ConceptPropertyUnique type, String id) {
        super(ErrorMessage.ID_NOT_UNIQUE.getMessage(concept.toString(), type.name(), id));
    }

    public ConceptIdNotUniqueException(Concept concept, String id){
        super(ErrorMessage.ID_ALREADY_TAKEN.getMessage(id, concept.toString()));
    }
}
