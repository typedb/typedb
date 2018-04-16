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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.kbs;

/*-
 * #%L
 * grakn-test-tools
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Thing;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.SampleKBLoader;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class AdmissionsKB extends TestKB {

    private static AttributeType<String> key;

    private static EntityType applicant;

    private static AttributeType<Long> TOEFL;
    private static AttributeType<Double> GPR;
    private static AttributeType<Long> GRE;
    private static AttributeType<Long> vGRE;
    private static AttributeType<String> specialHonours;
    private static AttributeType<String> degreeOrigin;
    private static AttributeType<String> transcript;
    private static AttributeType<String> priorGraduateWork;
    private static AttributeType<String> languageRequirement;
    private static AttributeType<String> considerGPR;
    private static AttributeType<String> admissionStatus;
    private static AttributeType<String> decisionType;

    public static SampleKBContext context() {
        return new AdmissionsKB().makeContext();
    }

    @Override
    protected void buildSchema(GraknTx tx) {
        key = tx.putAttributeType("name", AttributeType.DataType.STRING);

        TOEFL = tx.putAttributeType("TOEFL", AttributeType.DataType.LONG);
        GRE = tx.putAttributeType("GRE", AttributeType.DataType.LONG);
        vGRE = tx.putAttributeType("vGRE", AttributeType.DataType.LONG);
        GPR = tx.putAttributeType("GPR", AttributeType.DataType.DOUBLE);
        specialHonours = tx.putAttributeType("specialHonours", AttributeType.DataType.STRING);
        considerGPR = tx.putAttributeType("considerGPR", AttributeType.DataType.STRING);
        transcript = tx.putAttributeType("transcript", AttributeType.DataType.STRING);
        priorGraduateWork = tx.putAttributeType("priorGraduateWork", AttributeType.DataType.STRING);
        languageRequirement= tx.putAttributeType("languageRequirement", AttributeType.DataType.STRING);
        degreeOrigin = tx.putAttributeType("degreeOrigin", AttributeType.DataType.STRING);
        admissionStatus = tx.putAttributeType("admissionStatus", AttributeType.DataType.STRING);
        decisionType = tx.putAttributeType("decisionType", AttributeType.DataType.STRING);

        applicant = tx.putEntityType("applicant");
        applicant.attribute(TOEFL);
        applicant.attribute(GRE);
        applicant.attribute(vGRE);
        applicant.attribute(GPR);
        applicant.attribute(specialHonours);
        applicant.attribute(considerGPR);
        applicant.attribute(transcript);
        applicant.attribute(priorGraduateWork);
        applicant.attribute(languageRequirement);
        applicant.attribute(degreeOrigin);
        applicant.attribute(admissionStatus);
        applicant.attribute(decisionType);
        applicant.attribute(key);
    }

    @Override
    protected void buildInstances(GraknTx tx) {
        Thing Alice = putEntityWithResource(tx, "Alice", applicant, key.getLabel());
        Thing Bob = putEntityWithResource(tx, "Bob", applicant, key.getLabel());
        Thing Charlie = putEntityWithResource(tx, "Charlie", applicant, key.getLabel());
        Thing Denis = putEntityWithResource(tx, "Denis", applicant, key.getLabel());
        Thing Eva = putEntityWithResource(tx, "Eva", applicant, key.getLabel());
        Thing Frank = putEntityWithResource(tx, "Frank", applicant, key.getLabel());

        putResource(Alice, TOEFL, 470L);
        putResource(Alice, degreeOrigin, "nonUS");

        putResource(Bob, priorGraduateWork, "none");
        putResource(Bob, TOEFL, 520L);
        putResource(Bob, degreeOrigin, "US");
        putResource(Bob, transcript, "unavailable");
        putResource(Bob, specialHonours, "none");
        putResource(Bob, GRE, 1100L);

        putResource(Charlie, priorGraduateWork, "none");
        putResource(Charlie, TOEFL, 600L);
        putResource(Charlie, degreeOrigin, "US");
        putResource(Charlie, transcript, "available");
        putResource(Charlie, specialHonours, "none");
        putResource(Charlie, GRE, 1100L);
        putResource(Charlie, vGRE, 400L);
        putResource(Charlie, GPR, 2.99);

        putResource(Denis, priorGraduateWork, "none");
        putResource(Denis, degreeOrigin, "US");
        putResource(Denis, transcript, "available");
        putResource(Denis, specialHonours, "none");
        putResource(Denis, GRE, 900L);
        putResource(Denis, vGRE, 350L);
        putResource(Denis, GPR, 2.5);

        putResource(Eva, priorGraduateWork, "completed");
        putResource(Eva, specialHonours, "valedictorian");
        putResource(Eva, GPR, 3.0);

        putResource(Frank, TOEFL, 550L);
        putResource(Frank, degreeOrigin, "US");
        putResource(Frank, transcript, "unavailable");
        putResource(Frank, specialHonours, "none");
        putResource(Frank, GRE, 100L);
    }

    @Override
    protected void buildRelations() {

    }

    @Override
    protected void buildRules(GraknTx tx) {
        SampleKBLoader.loadFromFile(tx, "admission-rules.gql");
    }
}
