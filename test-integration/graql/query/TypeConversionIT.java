/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import grakn.core.concept.type.AttributeType;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlInsert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class TypeConversionIT {

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static SessionImpl session;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        try(TransactionOLTP tx = session.transaction().write()){
            tx.putAttributeType("resource-long", AttributeType.DataType.LONG);
            tx.putAttributeType("resource-double", AttributeType.DataType.DOUBLE);
            tx.putAttributeType("resource-float", AttributeType.DataType.FLOAT);
            tx.putAttributeType("resource-date", AttributeType.DataType.DATE);
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    private void verifyWrite(SessionImpl session, Pattern pattern){
        GraqlInsert insert = Graql.insert(pattern.statements());
        try(TransactionOLTP tx = session.transaction().write()){
            tx.execute(insert);
            tx.commit();
        }
    }

    private void verifyRead(SessionImpl session, Pattern pattern){
        try(TransactionOLTP tx = session.transaction().read()) {
            assertTrue(!tx.execute(Graql.match(pattern)).isEmpty());
        }
    }

    @Test
    public void whenAddressingDoubleAsInteger_ConversionHappens(){
        Pattern pattern = Graql.var("x").val(10).isa("resource-double");
        verifyWrite(session,pattern);
        //TODO verifyRead(session, pattern);
    }

    @Test
    public void whenAddressingDoubleAsLong_ConversionHappens(){
        Pattern pattern = Graql.var("x").val(10L).isa("resource-double");
        verifyWrite(session,pattern);
        //TODO verifyRead(session, pattern);
    }

    @Test
    public void whenAddressingDoubleAsFloat_ConversionHappens(){
        Pattern pattern = Graql.var("x").val(10f).isa("resource-double");
        verifyWrite(session,pattern);
        //TODO verifyRead(session, pattern);
    }

    @Test
    public void whenAddressingLongAsInt_ConversionHappens(){
        Pattern pattern = Graql.var("x").val(10).isa("resource-long");
        verifyWrite(session,pattern);
        //TODO verifyRead(session, pattern);
    }

    @Test
    public void whenAddressingDateAsLong_ConversionHappens(){
        Pattern pattern = Graql.var("x").val(10000000L).isa("resource-date");
        verifyWrite(session,pattern);
        //TODO verifyRead(session, pattern);
    }

    @Test (expected = TransactionException.class)
    public void whenAddressingDateAsNonLong_exceptionIsThrown() throws TransactionException{
        Pattern pattern = Graql.var("x").val(10000000.0).isa("resource-date");
        verifyWrite(session,pattern);
    }

    @Test (expected = TransactionException.class)
    public void whenAddressingDoubleAsBoolean_exceptionIsThrown() throws TransactionException{
        Pattern pattern = Graql.var("x").val(true).isa("resource-double");
        verifyWrite(session,pattern);
    }

    @Test (expected = TransactionException.class)
    public void whenAddressingLongAsDouble_exceptionIsThrown() throws TransactionException{
        Pattern pattern = Graql.var("x").val(10.0).isa("resource-long");
        verifyWrite(session,pattern);
    }
}
