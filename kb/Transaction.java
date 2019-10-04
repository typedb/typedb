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

package grakn.core.kb;

import grakn.core.concept.api.Concept;
import grakn.core.concept.api.ConceptId;
import grakn.core.concept.api.RelationType;
import grakn.core.concept.api.Role;
import grakn.core.concept.api.Label;
import grakn.core.concept.api.Rule;
import grakn.core.concept.api.SchemaConcept;
import grakn.core.concept.api.Type;
import grakn.core.kb.reasoner.cache.MultilevelSemanticCache;
import grakn.core.kb.cache.RuleCache;

public interface Transaction {

    SchemaConcept getSchemaConcept(Label label);

    Role getRole(String label);

    Concept getConcept(ConceptId conceptId);

    RuleCache ruleCache();

    RelationType getMetaRelationType();

    Role getMetaRole();

    Rule getMetaRule();

    Type getType(Label type);

    MultilevelSemanticCache queryCache();

    default long getShardCount(Type t) {
        return 1L;
    }
}
