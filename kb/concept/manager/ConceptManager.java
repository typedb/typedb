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

package grakn.core.kb.concept.manager;

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface ConceptManager {

    <T extends Concept> T buildConcept(Vertex vertex);
    <T extends Concept> T buildConcept(VertexElement vertex);

    <T extends Concept> T getConcept(ConceptId conceptId);

    <T extends Type> T getType(Label label);
    <T extends SchemaConcept> T getSchemaConcept(Label label);
    <T extends SchemaConcept> T getSchemaConcept(LabelId labelId);
    Rule getMetaRule();

    Role getRole(String label);
    RelationType getRelationType(String label);

    Type getMetaConcept();
    EntityType getMetaEntityType();
    RelationType getMetaRelationType();
    AttributeType getMetaAttributeType();

    // TODO Ideally we don't have this here?
    LabelId convertToId(Label label);
}
