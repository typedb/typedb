/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package generator;

import ai.grakn.GraknTx;
import strategy.AttributeStrategy;
import strategy.EntityStrategy;
import strategy.RelationshipStrategy;
import strategy.TypeStrategyInterface;

/**
 *
 */
public class GeneratorFactory {

    /**
     * @param typeStrategy
     * @param tx
     * @return
     */
    public GeneratorInterface create(TypeStrategyInterface typeStrategy, GraknTx tx) {
        /*

        We want to pass a structure like:
        TypeStrategy -> EntityGenerator
        RelationshipStrategy -> RelationshipGenerator
        AttributeStrategy -> AttributeGenerator

        that way when adding new generators this class doesn't need to be touched
         */

        if (typeStrategy instanceof EntityStrategy) {
            return new EntityGenerator((EntityStrategy) typeStrategy, tx);
        } else if (typeStrategy instanceof RelationshipStrategy) {
            return new RelationshipGenerator((RelationshipStrategy) typeStrategy, tx);
        } else if (typeStrategy instanceof AttributeStrategy) {
            return new AttributeGenerator((AttributeStrategy) typeStrategy, tx);
        }
        throw new RuntimeException("Couldn't find a matching Generator for this strategy");
    }

}
