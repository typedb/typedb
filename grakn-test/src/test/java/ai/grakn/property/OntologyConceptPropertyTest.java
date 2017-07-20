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

package ai.grakn.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.generator.AbstractOntologyConceptGenerator.Meta;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.stream.Stream;

import static ai.grakn.property.PropertyUtil.choose;
import static ai.grakn.property.PropertyUtil.directSubs;
import static ai.grakn.property.PropertyUtil.indirectSupers;
import static ai.grakn.util.ErrorMessage.CANNOT_DELETE;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.ErrorMessage.SUPER_LOOP_DETECTED;
import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
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
public class OntologyConceptPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenDeletingAMetaConcept_Throw(@Meta OntologyConcept ontologyConcept) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(isOneOf(
                META_TYPE_IMMUTABLE.getMessage(ontologyConcept.getLabel()),
                CANNOT_DELETE.getMessage(ontologyConcept.getLabel())
        ));
        ontologyConcept.delete();
    }

    @Property
    public void whenDeletingAnOntologyConceptWithDirectSubs_Throw(@NonMeta OntologyConcept ontologyConcept) {
        OntologyConcept superConcept = ontologyConcept.sup();
        assumeFalse(isMetaLabel(superConcept.getLabel()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(superConcept.getLabel()));
        superConcept.delete();
    }

    @Property
    public void whenCallingGetLabel_TheResultIsUnique(OntologyConcept concept1, @FromGraph OntologyConcept concept2) {
        assumeThat(concept1, not(is(concept2)));
        assertNotEquals(concept1.getLabel(), concept2.getLabel());
    }

    @Property
    public void whenCallingGetLabel_TheResultCanBeUsedToRetrieveTheSameConcept(
            @Open GraknGraph graph, @FromGraph OntologyConcept concept) {
        Label label = concept.getLabel();
        assertEquals(concept, graph.getOntologyConcept(label));
    }

    @Property
    public void whenAnOntologyElementHasADirectSuper_ItIsADirectSubOfThatSuper(OntologyConcept ontologyConcept) {
        OntologyConcept superConcept = ontologyConcept.sup();
        assumeTrue(superConcept != null);

        assertThat(directSubs(superConcept), hasItem(ontologyConcept));
    }

    @Property
    public void whenGettingSuper_TheResultIsNeverItself(OntologyConcept concept) {
        assertNotEquals(concept, concept.sup());
    }

    @Property
    public void whenAnOntologyConceptHasAnIndirectSuper_ItIsAnIndirectSubOfThatSuper(
            OntologyConcept subConcept, long seed) {
        OntologyConcept superConcept = choose(indirectSupers(subConcept), seed);
        assertThat((Collection<OntologyConcept>) superConcept.subs(), hasItem(subConcept));
    }

    @Property
    public void whenAnOntologyConceptHasAnIndirectSub_ItIsAnIndirectSuperOfThatSub(
            OntologyConcept superConcept, long seed) {
        OntologyConcept subConcept = choose(superConcept.subs(), seed);
        assertThat(indirectSupers(subConcept), hasItem(superConcept));
    }

    @Property
    public void whenGettingIndirectSub_ReturnSelfAndIndirectSubsOfDirectSub(@FromGraph OntologyConcept concept) {
        Collection<OntologyConcept> directSubs = directSubs(concept);
        OntologyConcept[] expected = Stream.concat(
                Stream.of(concept),
                directSubs.stream().flatMap(subConcept -> subConcept.subs().stream())
        ).toArray(OntologyConcept[]::new);

        assertThat(concept.subs(), containsInAnyOrder(expected));
    }

    @Property
    public void whenGettingTheIndirectSubs_TheyContainTheOntologyConcept(OntologyConcept concept) {
        assertThat((Collection<OntologyConcept>) concept.subs(), hasItem(concept));
    }

    @Property
    public void whenSettingTheDirectSuperOfAMetaConcept_Throw(
            @Meta OntologyConcept subConcept, @FromGraph OntologyConcept superConcept) {
        assumeTrue(sameOntologyConcept(subConcept, superConcept));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subConcept.getLabel()));
        setDirectSuper(subConcept, superConcept);
    }

    @Property
    public void whenSettingTheDirectSuperToAnIndirectSub_Throw(
            @NonMeta OntologyConcept concept, long seed) {
        OntologyConcept newSuperConcept = choose(concept.subs(), seed);

        exception.expect(GraphOperationException.class);
        exception.expectMessage(SUPER_LOOP_DETECTED.getMessage(concept.getLabel(), newSuperConcept.getLabel()));
        setDirectSuper(concept, newSuperConcept);
    }

    @Property
    public void whenSettingTheDirectSuper_TheDirectSuperIsSet(
            @NonMeta OntologyConcept subConcept, @FromGraph OntologyConcept superConcept) {
        assumeTrue(sameOntologyConcept(subConcept, superConcept));
        assumeThat((Collection<OntologyConcept>) subConcept.subs(), not(hasItem(superConcept)));

        setDirectSuper(subConcept, superConcept);

        assertEquals(superConcept, subConcept.sup());
    }

    @Property
    public void whenAddingADirectSubThatIsAMetaConcept_Throw(
            OntologyConcept superConcept, @Meta @FromGraph OntologyConcept subConcept) {
        assumeTrue(sameOntologyConcept(subConcept, superConcept));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subConcept.getLabel()));
        addDirectSub(superConcept, subConcept);
    }

    @Property
    public void whenAddingADirectSubWhichIsAnIndirectSuper_Throw(
            @NonMeta OntologyConcept newSubConcept, long seed) {
        OntologyConcept concept = choose(newSubConcept.subs(), seed);

        exception.expect(GraphOperationException.class);
        exception.expectMessage(SUPER_LOOP_DETECTED.getMessage(newSubConcept.getLabel(), concept.getLabel()));
        addDirectSub(concept, newSubConcept);
    }

    @Property
    public void whenAddingADirectSub_TheDirectSubIsAdded(
            OntologyConcept superConcept, @NonMeta @FromGraph OntologyConcept subConcept) {
        assumeTrue(sameOntologyConcept(subConcept, superConcept));
        assumeThat((Collection<OntologyConcept>) subConcept.subs(), not(hasItem(superConcept)));

        addDirectSub(superConcept, subConcept);

        assertThat(directSubs(superConcept), hasItem(subConcept));
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingAnOntologyConceptWithHypothesisRules_Throw(OntologyConcept concept) {
        assumeThat(concept.getRulesOfHypothesis(), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(concept.getLabel()));
        concept.delete();
    }

    @Ignore // TODO: Find a way to generate linked rules
    @Property
    public void whenDeletingAnOntologyConceptWithConclusionRules_Throw(OntologyConcept concept) {
        assumeThat(concept.getRulesOfConclusion(), not(empty()));

        exception.expect(GraphOperationException.class);
        exception.expectMessage(CANNOT_DELETE.getMessage(concept.getLabel()));
        concept.delete();
    }


    private boolean sameOntologyConcept(OntologyConcept concept1, OntologyConcept concept2) {
        return concept1.isEntityType() && concept2.isEntityType() ||
                concept1.isRelationType() && concept2.isRelationType() ||
                concept1.isRole() && concept2.isRole() ||
                concept1.isResourceType() && concept2.isResourceType() ||
                concept1.isRuleType() && concept2.isRuleType();
    }

    private void setDirectSuper(OntologyConcept subConcept, OntologyConcept superConcept) {
        if (subConcept.isEntityType()) {
            subConcept.asEntityType().sup(superConcept.asEntityType());
        } else if (subConcept.isRelationType()) {
            subConcept.asRelationType().sup(superConcept.asRelationType());
        } else if (subConcept.isRole()) {
            subConcept.asRole().sup(superConcept.asRole());
        } else if (subConcept.isResourceType()) {
            subConcept.asResourceType().sup(superConcept.asResourceType());
        } else if (subConcept.isRuleType()) {
            subConcept.asRuleType().sup(superConcept.asRuleType());
        } else {
            fail("unreachable");
        }
    }

    private void addDirectSub(OntologyConcept superConcept, OntologyConcept subConcept) {
        if (superConcept.isEntityType()) {
            superConcept.asEntityType().sub(subConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            superConcept.asRelationType().sub(subConcept.asRelationType());
        } else if (superConcept.isRole()) {
            superConcept.asRole().sub(subConcept.asRole());
        } else if (superConcept.isResourceType()) {
            superConcept.asResourceType().sub(subConcept.asResourceType());
        } else if (superConcept.isRuleType()) {
            superConcept.asRuleType().sub(subConcept.asRuleType());
        } else {
            fail("unreachable");
        }
    }
}
