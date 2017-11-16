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

package ai.grakn.engine.controller.response;

import ai.grakn.concept.Label;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.SchemaConcept}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public abstract class SchemaConcept extends Concept{

    @Nullable
    public abstract SchemaConcept superConcept();

    @Nullable
    public abstract Set<SchemaConcept> subConcepts();

    @JsonProperty
    public abstract Label label();

    @Nullable
    @JsonProperty("implicit")
    public abstract Boolean isImplicit();

    @JsonProperty("super")
    public String sup(){
        return superConcept().id();
    }

    @JsonProperty
    public Set<String> subs() {
        return extractIds(subConcepts());
    }

    @Override
    public String uniqueId(){
        return label().getValue();
    }
}

