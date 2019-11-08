/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.kb.graql.executor;

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.server.exception.GraqlSemanticException;
import grakn.core.kb.server.exception.InvalidKBException;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

public interface ConceptBuilder {


    /**
     * Make the second argument the super of the first argument
     *
     * @throws GraqlSemanticException if the types are different, or setting the super to be a meta-type
     */
    static void setSuper(SchemaConcept subConcept, SchemaConcept superConcept) {
        if (superConcept.isEntityType()) {
            subConcept.asEntityType().sup(superConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            subConcept.asRelationType().sup(superConcept.asRelationType());
        } else if (superConcept.isRole()) {
            subConcept.asRole().sup(superConcept.asRole());
        } else if (superConcept.isAttributeType()) {
            subConcept.asAttributeType().sup(superConcept.asAttributeType());
        } else if (superConcept.isRule()) {
            subConcept.asRule().sup(superConcept.asRule());
        } else {
            throw InvalidKBException.insertMetaType(subConcept.label(), superConcept);
        }
    }

    ConceptBuilder isa(Type type);

    ConceptBuilder sub(SchemaConcept superConcept);

    ConceptBuilder isRole();

    ConceptBuilder isRule();

    ConceptBuilder label(Label label);

    ConceptBuilder id(ConceptId id);

    ConceptBuilder value(Object value);

    ConceptBuilder dataType(AttributeType.DataType<?> dataType);

    ConceptBuilder when(Pattern when);

    ConceptBuilder then(Pattern then);

    /**
     * Build the Concept and return it, using the properties given.
     *
     * @throws GraqlSemanticException if the properties provided are inconsistent
     */
    Concept build();

}
