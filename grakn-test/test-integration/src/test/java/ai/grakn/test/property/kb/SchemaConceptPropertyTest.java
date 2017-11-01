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

package ai.grakn.test.property.kb;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.generator.AbstractSchemaConceptGenerator.Meta;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
import ai.grakn.generator.FromTxGenerator.FromTx;
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.test.property.PropertyUtil;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.stream.Stream;

import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * @author Felix Chapman
 */
@RunWith(JUnitQuickcheck.class)
public class SchemaConceptPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenDeletingAMetaConcept_Throw(@Meta SchemaConcept schemaConcept) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(isOneOf(
                GraknTxOperationException.metaTypeImmutable(schemaConcept.getLabel()).getMessage(),
                GraknTxOperationException.cannotBeDeleted(schemaConcept).getMessage()
        ));
        schemaConcept.delete();
    }

    @Property
    public void whenDeletingASchemaConceptWithDirectSubs_Throw(@NonMeta SchemaConcept schemaConcept) {
        SchemaConcept superConcept = schemaConcept.sup();
        assumeFalse(isMetaLabel(superConcept.getLabel()));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.cannotBeDeleted(superConcept).getMessage());
        superConcept.delete();
    }

    @Property
    public void whenCallingGetLabel_TheResultIsUnique(SchemaConcept concept1, @FromTx SchemaConcept concept2) {
        assumeThat(concept1, not(is(concept2)));
        assertNotEquals(concept1.getLabel(), concept2.getLabel());
    }

    @Property
    public void whenCallingGetLabel_TheResultCanBeUsedToRetrieveTheSameConcept(
            @Open GraknTx graph, @FromTx SchemaConcept concept) {
        Label label = concept.getLabel();
        assertEquals(concept, graph.getSchemaConcept(label));
    }

    @Property
    public void whenASchemaElementHasADirectSuper_ItIsADirectSubOfThatSuper(SchemaConcept schemaConcept) {
        SchemaConcept superConcept = schemaConcept.sup();
        assumeTrue(superConcept != null);

        assertThat(PropertyUtil.directSubs(superConcept), hasItem(schemaConcept));
    }

    @Property
    public void whenGettingSuper_TheResultIsNeverItself(SchemaConcept concept) {
        assertNotEquals(concept, concept.sup());
    }

    @Property
    public void whenASchemaConceptHasAnIndirectSuper_ItIsAnIndirectSubOfThatSuper(
            SchemaConcept subConcept, long seed) {
        SchemaConcept superConcept = PropertyUtil.choose(PropertyUtil.indirectSupers(subConcept), seed);
        assertThat(superConcept.subs().collect(toSet()), hasItem(subConcept));
    }

    @Property
    public void whenASchemaConceptHasAnIndirectSub_ItIsAnIndirectSuperOfThatSub(
            SchemaConcept superConcept, long seed) {
        SchemaConcept subConcept = PropertyUtil.choose(superConcept.subs(), seed);
        assertThat(PropertyUtil.indirectSupers(subConcept), hasItem(superConcept));
    }

    @Property
    public void whenGettingIndirectSub_ReturnSelfAndIndirectSubsOfDirectSub(@FromTx SchemaConcept concept) {
        Collection<SchemaConcept> directSubs = PropertyUtil.directSubs(concept);
        SchemaConcept[] expected = Stream.concat(
                Stream.of(concept),
                directSubs.stream().flatMap(SchemaConcept::subs)
        ).toArray(SchemaConcept[]::new);

        assertThat(concept.subs().collect(toSet()), containsInAnyOrder(expected));
    }

    @Property
    public void whenGettingTheIndirectSubs_TheyContainTheSchemaConcept(SchemaConcept concept) {
        assertThat(concept.subs().collect(toSet()), hasItem(concept));
    }

    @Property
    public void whenSettingTheDirectSuperOfAMetaConcept_Throw(
            @Meta SchemaConcept subConcept, @FromTx SchemaConcept superConcept) {
        assumeTrue(sameSchemaConcept(subConcept, superConcept));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(subConcept.getLabel()).getMessage());
        setDirectSuper(subConcept, superConcept);
    }

    @Property
    public void whenSettingTheDirectSuperToAnIndirectSub_Throw(
            @NonMeta SchemaConcept concept, long seed) {
        SchemaConcept newSuperConcept = PropertyUtil.choose(concept.subs(), seed);

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.loopCreated(concept, newSuperConcept).getMessage());
        setDirectSuper(concept, newSuperConcept);
    }

    @Property
    public void whenSettingTheDirectSuper_TheDirectSuperIsSet(
            @NonMeta SchemaConcept subConcept, @FromTx SchemaConcept superConcept) {
        assumeTrue(sameSchemaConcept(subConcept, superConcept));
        assumeThat(subConcept.subs().collect(toSet()), not(hasItem(superConcept)));

        //TODO: get rid of this once traversing to the instances of an implicit type does not require  the plays edge
        if(subConcept.isType()) assumeThat(subConcept.asType().sup().instances().collect(toSet()), is(empty()));

        setDirectSuper(subConcept, superConcept);

        assertEquals(superConcept, subConcept.sup());
    }

    @Property
    public void whenAddingADirectSubThatIsAMetaConcept_Throw(
            SchemaConcept superConcept, @Meta @FromTx SchemaConcept subConcept) {
        assumeTrue(sameSchemaConcept(subConcept, superConcept));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(subConcept.getLabel()).getMessage());
        addDirectSub(superConcept, subConcept);
    }

    @Property
    public void whenAddingADirectSubWhichIsAnIndirectSuper_Throw(
            @NonMeta SchemaConcept newSubConcept, long seed) {
        SchemaConcept concept = PropertyUtil.choose(newSubConcept.subs(), seed);

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.loopCreated(newSubConcept, concept).getMessage());
        addDirectSub(concept, newSubConcept);
    }

    @Property
    public void whenAddingADirectSub_TheDirectSubIsAdded(
            SchemaConcept superConcept, @NonMeta @FromTx SchemaConcept subConcept) {
        assumeTrue(sameSchemaConcept(subConcept, superConcept));
        assumeThat(subConcept.subs().collect(toSet()), not(hasItem(superConcept)));

        //TODO: get rid of this once traversing to the instances of an implicit type does not require  the plays edge
        if(subConcept.isType()) assumeThat(subConcept.asType().sup().instances().collect(toSet()), is(empty()));

        addDirectSub(superConcept, subConcept);

        assertThat(PropertyUtil.directSubs(superConcept), hasItem(subConcept));
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingASchemaConceptWithHypothesisRules_Throw(SchemaConcept concept) {
        assumeThat(concept.getRulesOfHypothesis().collect(toSet()), not(empty()));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.cannotBeDeleted(concept).getMessage());
        concept.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingASchemaConceptWithConclusionRules_Throw(SchemaConcept concept) {
        assumeThat(concept.getRulesOfConclusion().collect(toSet()), not(empty()));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.cannotBeDeleted(concept).getMessage());
        concept.delete();
    }


    private boolean sameSchemaConcept(SchemaConcept concept1, SchemaConcept concept2) {
        return concept1.isEntityType() && concept2.isEntityType() ||
                concept1.isRelationshipType() && concept2.isRelationshipType() ||
                concept1.isRole() && concept2.isRole() ||
                concept1.isAttributeType() && concept2.isAttributeType() ||
                concept1.isRule() && concept2.isRule();
    }

    private void setDirectSuper(SchemaConcept subConcept, SchemaConcept superConcept) {
        if (subConcept.isEntityType()) {
            subConcept.asEntityType().sup(superConcept.asEntityType());
        } else if (subConcept.isRelationshipType()) {
            subConcept.asRelationshipType().sup(superConcept.asRelationshipType());
        } else if (subConcept.isRole()) {
            subConcept.asRole().sup(superConcept.asRole());
        } else if (subConcept.isAttributeType()) {
            subConcept.asAttributeType().sup(superConcept.asAttributeType());
        } else if (subConcept.isRule()) {
            subConcept.asRule().sup(superConcept.asRule());
        } else {
            fail("unreachable");
        }
    }

    private void addDirectSub(SchemaConcept superConcept, SchemaConcept subConcept) {
        if (superConcept.isEntityType()) {
            superConcept.asEntityType().sub(subConcept.asEntityType());
        } else if (superConcept.isRelationshipType()) {
            superConcept.asRelationshipType().sub(subConcept.asRelationshipType());
        } else if (superConcept.isRole()) {
            superConcept.asRole().sub(subConcept.asRole());
        } else if (superConcept.isAttributeType()) {
            superConcept.asAttributeType().sub(subConcept.asAttributeType());
        } else if (superConcept.isRule()) {
            superConcept.asRule().sub(subConcept.asRule());
        } else {
            fail("unreachable");
        }
    }
}