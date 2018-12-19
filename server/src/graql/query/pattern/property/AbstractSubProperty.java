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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;
import java.util.stream.Stream;

public abstract class AbstractSubProperty extends VarProperty {

    public abstract Statement superType();

    @Override
    public String getProperty() {
        return superType().getPrintableName();
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public Stream<Statement> getTypes() {
        return Stream.of(superType());
    }

    @Override
    public Stream<Statement> innerStatements() {
        return Stream.of(superType());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            SchemaConcept concept = executor.get(var).asSchemaConcept();

            SchemaConcept expectedSuperConcept = executor.get(superType().var()).asSchemaConcept();
            SchemaConcept actualSuperConcept = concept.sup();

            if (!concept.isDeleted() && expectedSuperConcept.equals(actualSuperConcept)) {
                concept.delete();
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, superType().var()).build());
    }
}
