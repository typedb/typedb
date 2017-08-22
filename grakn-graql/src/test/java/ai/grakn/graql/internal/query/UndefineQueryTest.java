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
 *
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
public class UndefineQueryTest {

    private static final VarPattern THING = Graql.label(Schema.MetaSchema.THING.getLabel());
    private static final VarPattern ENTITY = Graql.label(Schema.MetaSchema.ENTITY.getLabel());
    public static final Label NEW_TYPE = Label.of("new-type");

    @ClassRule
    public static final SampleKBContext movieKB = SampleKBContext.preLoad(MovieKB.get());
    public static final Var x = var("x");

    private QueryBuilder qb;
    private GraknTx tx;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        tx = movieKB.tx();
        qb = tx.graql();


    }

    @Test
    public void whenUndefiningSub_TheSchemaConceptIsDeleted() {
        qb.define(label(NEW_TYPE).sub(ENTITY)).execute();

        assertNotNull(tx.getType(NEW_TYPE));

        qb.undefine(label(NEW_TYPE).sub(ENTITY)).execute();

        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningSubWhichDoesntExist_Throw() {
        qb.define(label(NEW_TYPE).sub(ENTITY)).execute();

        exception.expect(RuntimeException.class); // TODO
        qb.undefine(label(NEW_TYPE).sub(THING)).execute();
    }

    @Test
    public void whenUndefiningHas_TheHasLinkIsDeleted() {
        qb.define(label(NEW_TYPE).sub(ENTITY).has("name")).execute();

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), hasItemInArray(tx.getAttributeType("name")));

        qb.undefine(label(NEW_TYPE).has("name")).execute();

        assertThat(tx.getType(NEW_TYPE).attributes().toArray(), not(hasItemInArray(tx.getAttributeType("name"))));
    }

    @Test
    public void whenUndefiningHasWhichDoesntExist_Throw() {
        qb.define(label(NEW_TYPE).sub(ENTITY).has("name")).execute();

        exception.expect(RuntimeException.class); // TODO
        qb.undefine(label(NEW_TYPE).has("title")).execute();
    }

    @Test
    public void whenUndefiningById_TheSchemaConceptIsDeleted() {
        Type newType = qb.define(x.label(NEW_TYPE).sub(ENTITY)).execute().get(x).asType();

        assertNotNull(tx.getType(NEW_TYPE));

        qb.undefine(var().id(newType.getId()).sub(ENTITY)).execute();

        assertNull(tx.getType(NEW_TYPE));
    }

    @Test
    public void whenUndefiningDataType_Throw() {
        exception.expect(RuntimeException.class); // TODO
        qb.undefine(label("name").datatype(AttributeType.DataType.STRING)).execute();
    }
}
