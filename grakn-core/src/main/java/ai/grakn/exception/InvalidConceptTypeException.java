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
import ai.grakn.concept.Resource;

/**
 * This exception is thrown when attempting to incorrectly cast a concept to something it is not.
 * For example when using {@link Concept#asEntityType()} on a {@link Resource}
 *
 * @author Filipe Teixiera
 */
public class InvalidConceptTypeException extends ConceptException {
    public InvalidConceptTypeException(String message) {
        super(message);
    }
}
