/*
 * Copyright (C) 2020 Grakn Labs
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
 */

package grakn.core.graql.query;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlInsert;
import graql.lang.query.MatchClause;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/*
Test Java-based Graql builder syntax for writing attribute values
The Graql-only compatibility tests have been moved to BDD
 */
public class NumberCastingIT {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static Session session;

    @Before
    public void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putAttributeType("attr-long", AttributeType.ValueType.LONG);
            tx.putAttributeType("attr-double", AttributeType.ValueType.DOUBLE);
            tx.putAttributeType("attr-date", AttributeType.ValueType.DATETIME);
            tx.commit();
        }
    }

    @After
    public void closeSession() {
        session.close();
    }

    private void verifyWrite(Session session, Pattern pattern) {
        GraqlInsert insert = Graql.insert(pattern.statements());
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(insert);
            tx.commit();
        }
    }

    private void verifyRead(Session session, Pattern pattern) {
        MatchClause match = Graql.match(pattern.statements());
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            List<ConceptMap> answers = tx.execute(match);
            assertFalse(answers.isEmpty());
        }
    }

    private void cleanup(Session session, Pattern pattern) {
        MatchClause match = Graql.match(pattern.statements());
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            for (Variable var : pattern.variables()) {
                tx.execute(match.delete(Graql.var(var).isa("thing")));
            }
            tx.commit();
        }
    }

    @Test
    public void whenAddressingDoubleAsInteger_ConversionHappens() {
        Pattern pattern = Graql.var("x").val(10).isa("attr-double");
        verifyWrite(session, pattern);
        verifyRead(session, pattern);
        cleanup(session, pattern);
    }

    @Test
    public void whenAddressingDoubleAsLong_ConversionHappens() {
        Pattern pattern = Graql.var("x").val(10L).isa("attr-double");
        verifyWrite(session, pattern);
        verifyRead(session, pattern);
        cleanup(session, pattern);
    }

    @Test
    public void whenAddressingLongAsInt_ConversionHappens() {
        Pattern pattern = Graql.var("x").val(10).isa("attr-long");
        verifyWrite(session, pattern);
        verifyRead(session, pattern);
        cleanup(session, pattern);
    }

    @Test
    public void whenAddressingDateAsLocalDate_ConversionHappens() {
        LocalDate now = LocalDate.now();

        Pattern pattern = Graql.parsePattern("$x " + now + " isa attr-date;");
        verifyWrite(session, pattern);
        verifyRead(session, pattern);
        cleanup(session, pattern);
    }

    @Test
    public void whenAddressingDateAsLocalDateTime_ConversionHappens() {
        LocalDateTime now = LocalDateTime.now();
        Pattern pattern = Graql.parsePattern("$x " + now + " isa attr-date;");
        verifyWrite(session, pattern);
        verifyRead(session, pattern);
        cleanup(session, pattern);
    }

    @Test
    public void whenReadingLongAsDouble_ConversionHappens() {
        double value = 10.0;
        Pattern pattern = Graql.var("x").val(value).isa("attr-long");
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.getAttributeType("attr-long").create(10L);
            tx.commit();
        }
        verifyRead(session, pattern);
    }

    @Test
    public void whenWritingLongAsDouble_throw() {
        double value = 10.0;
        Pattern pattern = Graql.var("x").val(value).isa("attr-long");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(ErrorMessage.INVALID_VALUETYPE_WRITE.getMessage(value, "Double", AttributeType.ValueType.LONG.name(), "attr-long"));
        verifyWrite(session, pattern);
    }

    @Test
    public void whenAddressingDateAsNonDate_exceptionIsThrown() throws GraknConceptException {
        double value = 10000000.0;
        Pattern pattern = Graql.var("x").val(value).isa("attr-date");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(ErrorMessage.INVALID_VALUETYPE_WRITE.getMessage(value, "Double", AttributeType.ValueType.DATETIME, "attr-date"));
        verifyWrite(session, pattern);
    }

    @Test
    public void whenAddressingDoubleAsBoolean_exceptionIsThrown() throws GraknConceptException {
        boolean value = true;
        Pattern pattern = Graql.var("x").val(value).isa("attr-double");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(ErrorMessage.INVALID_VALUETYPE_WRITE.getMessage( value, "Boolean", AttributeType.ValueType.DOUBLE, "attr-double"));
        verifyWrite(session, pattern);
    }

    @Test
    public void whenAddressingLongAsDouble_exceptionIsThrown() throws GraknConceptException {
        double value = 10.1;
        Pattern pattern = Graql.var("x").val(value).isa("attr-long");
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(ErrorMessage.INVALID_VALUETYPE_WRITE.getMessage( value, "Double", AttributeType.ValueType.LONG, "attr-long"));
        verifyWrite(session, pattern);
    }
}
