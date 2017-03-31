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

package ai.grakn.exception;

import ai.grakn.concept.Concept;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;

/**
 * This exception is thrown when two concepts attept to have the same unique id.
 *
 * @author Filipe Teixeira
 */
public class ConceptNotUniqueException extends ConceptException {
    public ConceptNotUniqueException(Concept concept, Schema.ConceptProperty type, String id) {
        super(ErrorMessage.ID_NOT_UNIQUE.getMessage(concept.toString(), type.name(), id));
    }

    public ConceptNotUniqueException(Concept concept, String id){
        super(ErrorMessage.ID_ALREADY_TAKEN.getMessage(id, concept.toString()));
    }

    public ConceptNotUniqueException(String message){
        super(message);
    }
}
