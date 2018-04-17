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
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarPatternAdmin;

import static ai.grakn.graql.Graql.var;

/**
 * @author Felix Chapman
 */
public class RelationPlayers extends AbstractGenerator<RelationPlayer> {

    public RelationPlayers() {
        super(RelationPlayer.class);
    }

    @Override
    public RelationPlayer generate() {
        if (random.nextBoolean()) {
            return RelationPlayer.of(gen(VarPatternAdmin.class));
        } else {
            VarPatternAdmin varPattern;

            if (random.nextBoolean()) {
                varPattern = var().label(gen(Label.class)).admin();
            } else {
                varPattern = gen(VarPatternAdmin.class);
            }

            return RelationPlayer.of(varPattern, gen(VarPatternAdmin.class));
        }
    }
}