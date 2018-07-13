/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.generator;

import ai.grakn.concept.Label;
import ai.grakn.concept.Rule;
import ai.grakn.graql.QueryBuilder;

/**
 * A generator that produces random {@link Rule}s
 *
 * @author Felix Chapman
 */
public class Rules extends AbstractSchemaConceptGenerator<Rule> {

    public Rules() {
        super(Rule.class);
    }

    @Override
    protected Rule newSchemaConcept(Label label) {
        // TODO: generate more complicated rules
        QueryBuilder graql = this.tx().graql();
        return tx().putRule(label, graql.parser().parsePattern("$x"), graql.parser().parsePattern("$x"));
    }

    @Override
    protected Rule metaSchemaConcept() {
        return tx().admin().getMetaRule();
    }
}
