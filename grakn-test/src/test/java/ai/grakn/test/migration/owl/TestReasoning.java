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

package ai.grakn.test.migration.owl;

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.migration.owl.OwlModel;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.semanticweb.HermiT.Configuration;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class TestReasoning extends TestOwlGraknBase {

    private final IRI baseIri = IRI.create("http://www.co-ode.org/roberts/family-tree.owl");
    private OWLReasoner hermit;

    @Before
    public void loadOwlFiles() throws GraknValidationException {
        OWLOntology family = loadOntologyFromResource("owl", "family.owl");
        migrator.ontology(family).graph(graph).migrate();
        migrator.graph().commit();
        hermit = new org.semanticweb.HermiT.Reasoner(new Configuration(), family);
    }

    //infer all subjects of relation relationIRI with object 'instanceId'
    private QueryAnswers inferRelationOWL(IRI relationIRI, String instanceId, OWLReasoner reasoner) {
        IRI instance = baseIri.resolve("#" + instanceId);

        OWLDataFactory df = manager.getOWLDataFactory();
        OWLClass person =  df.getOWLClass(baseIri.resolve("#Person"));
        OWLObjectProperty relation = df.getOWLObjectProperty(relationIRI);

        long owlStartTime = System.currentTimeMillis();
        OWLClassExpression expr = df.getOWLObjectIntersectionOf(
                person,
                df.getOWLObjectHasValue(relation, df.getOWLNamedIndividual(instance)));
        Set<OWLNamedIndividual> owlResult = reasoner.getInstances(expr).entities().collect(Collectors.toSet());
        long owlTime = System.currentTimeMillis() - owlStartTime;

        QueryAnswers OWLanswers = new QueryAnswers();
        owlResult.forEach(result -> {
            Answer resultMap = new QueryAnswer();
            resultMap.put(VarName.of("x"), migrator.entity(result));
            OWLanswers.add(resultMap);
        });

        System.out.println(reasoner.toString() + " answers: " + OWLanswers.size() + " in " + owlTime + " ms");
        return new QueryAnswers(OWLanswers);
    }

    private QueryAnswers inferRelationGrakn(String relationId, String instanceId) {
        QueryBuilder qb = migrator.graph().graql().infer(true).materialise(false);
        long gknStartTime = System.currentTimeMillis();
        String subjectRoleId = "owl-subject-" + relationId;
        String objectRoleId = "owl-object-" + relationId;

        //match $x isa tPerson; $x has name $name;
        //$y has name 'instance';(owl-subject-relationId: $x, owl-object-relationId: $y) isa relationId;
        MatchQuery query = qb.match(
                var("x").isa("tPerson"),
                var("y").has(OwlModel.IRI.owlname(), "e"+instanceId),
                var().isa(relationId).rel(subjectRoleId, "x").rel(objectRoleId, "y") ).select("x");
        QueryAnswers gknAnswers = queryAnswers(query);
        long gknTime = System.currentTimeMillis() - gknStartTime;
        System.out.println("Grakn Reasoner answers: " + gknAnswers.size() + " in " + gknTime + " ms");
        return gknAnswers;
    }

    @Ignore //TODO: Fix this test. Not sure why it is not working remotely
    @Test
    public void testFullReasoning(){
        QueryBuilder qb = migrator.graph().graql().infer(false);
        QueryBuilder iqb = migrator.graph().graql().infer(true).materialise(false);
        String richardId = "richard_henry_steward_1897";
        String hasGreatUncleId = "op-hasGreatUncle";
        String explicitQuery = "match $x isa tPerson;" +
                "{$x has owl-iri 'erichard_john_bright_1962';} or {$x has owl-iri 'erobert_david_bright_1965';};";
        assertEquals(inferRelationGrakn(hasGreatUncleId, richardId), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery)));

        String queryString2 = "match (owl-subject-op-hasGreatUncle: $x, owl-object-op-hasGreatUncle: $y) isa op-hasGreatUncle;" +
                "$x has owl-iri 'eethel_archer_1912'; select $y;";
        String explicitQuery2 = "match $y isa tPerson;"+
                "{$y has owl-iri 'eharry_whitfield_1854';} or" +
                "{$y has owl-iri 'ejames_whitfield_1848';} or" +
                "{$y has owl-iri 'ewalter_whitfield_1863';} or" +
                "{$y has owl-iri 'ewilliam_whitfield_1852';} or" +
                "{$y has owl-iri 'egeorge_whitfield_1865';};";
        assertEquals(iqb.<MatchQuery>parse(queryString2).stream(), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery2)).stream());

        String queryString3 = "match (owl-subject-op-hasGreatAunt: $x, owl-object-op-hasGreatAunt: $y) isa op-hasGreatAunt;" +
                "$x has owl-iri 'emary_kate_green_1865'; select $y;";
        String explicitQuery3= "match $y isa tPerson;{$y has owl-iri 'etamar_green_1810';} or" +
                "{$y has owl-iri 'ezilpah_green_1810';} or {$y has owl-iri 'eelizabeth_pickard_1805';} or" +
                "{$y has owl-iri 'esarah_ingelby_1821';} or {$y has owl-iri 'eann_pickard_1809';} or" +
                "{$y has owl-iri 'esusanna_pickard_1803';} or {$y has owl-iri 'emary_green_1803';} or" +
                "{$y has owl-iri 'erebecca_green_1800';} or {$y has owl-iri 'eann_green_1806';};";
        assertEquals(iqb.<MatchQuery>parse(queryString3).stream(), Sets.newHashSet(qb.<MatchQuery>parse(explicitQuery3)).stream());

        IRI hasAncestor = baseIri.resolve("#hasAncestor");
        String hasAncestorId = "op-hasAncestor";
        String isAncestorOfId = "op-isAncestorOf";

        String eleanorId = "eleanor_pringle_1741";
        assertEquals(inferRelationOWL(hasAncestor, eleanorId, hermit), inferRelationGrakn(hasAncestorId, eleanorId));

        String elisabethId = "elizabeth_clamper_1760";
        String explicitElisabethQuery = "match $x isa tPerson, has owl-iri $iri;" +
                "{$iri value 'ethomas_john_bright_1988';} or {$iri value 'emartin_dowse_1944';} or" +
                "{$iri value 'ejames_archer_1840';} or {$iri value 'ejulie_bright_1966';} or" +
                "{$iri value 'edavid_bright_1934';} or {$iri value 'ejune_dowse_1941';} or" +
                "{$iri value 'erichard_john_bright_1962';} or {$iri value 'ejames_bright_1964';} or" +
                "{$iri value 'eanne_archer_1964';} or {$iri value 'ewilliam_archer_1801';} or" +
                "{$iri value 'epeter_william_bright_1941';} or {$iri value 'ejames_alexander_archer_1882';} or" +
                "{$iri value 'eyvonne_archer_1940';} or {$iri value 'ewilliam_archer_1832';} or" +
                "{$iri value 'eiris_ellen_archer_1906';} or {$iri value 'eavril_bright_1990';} or" +
                "{$iri value 'ejane_archer';} or {$iri value 'ejean_margaret_archer_1934';} or" +
                "{$iri value 'eethel_archer_1912';} or {$iri value 'ejohn_bright_1930';} or" +
                "{$iri value 'ewilliam_bright_2001';} or {$iri value 'ejohn_archer_1804';} or" +
                "{$iri value 'ejane_archer_1837';} or {$iri value 'emary_archer_1885';} or" +
                "{$iri value 'eroy_cleife_1944';} or {$iri value 'emark_bright_1956';} or" +
                "{$iri value 'ejames_keith_archer_1946';} or {$iri value 'epaul_archer_1950';} or" +
                "{$iri value 'elily_archer_1880';} or {$iri value 'ejanet_bright_1964';} or" +
                "{$iri value 'echristopher_archer_1849';} or {$iri value 'ethomas_archer_1849';} or" +
                "{$iri value 'ealec_john_archer_1927';} or {$iri value 'emaureen_dowse_1939';} or" +
                "{$iri value 'eclare_bright_1966';} or {$iri value 'ejohn_english_archer';} or" +
                "{$iri value 'ejoyce_archer_1921';} or {$iri value 'ealan_john_dowse_1936';} or" +
                "{$iri value 'ejane_archer_1837';} or {$iri value 'emary_archer_1885';} or" +
                "{$iri value 'eian_alexander_archer_1944';} or {$iri value 'ewilliam_bright_1970';} or" +
                "{$iri value 'emary_archer_1850';} or {$iri value 'eellen_archer_1875';} or" +
                "{$iri value 'ejames_archer_1887';} or {$iri value 'edorothy_archer_1845';} or" +
                "{$iri value 'eian_bright_1959';} or {$iri value 'ethomas_archer_1868';} or" +
                "{$iri value 'erobert_david_bright_1965';} or {$iri value 'esheila_cleife_1949';} or" +
                "{$iri value 'ejohn_archer_1835';} or {$iri value 'eelizabeth_archer_1843';} or" +
                "{$iri value 'enorman_james_archer_1909';} or {$iri value 'ereece_bright_1993';}; select $x;";
        QueryAnswers elisabethAnswers = inferRelationGrakn(hasAncestorId, elisabethId);
        assertEquals(elisabethAnswers, Sets.newHashSet(qb.<MatchQuery>parse(explicitElisabethQuery)));

        String anneId = "anne_archer_1964";
        String explicitAnneQuery = "match $x isa tPerson, has owl-iri $iri;" +
                "{$iri value 'ejane_blake_1784';} or {$iri value 'esarah_jacobs_1834';} or" +
                "{$iri value 'eharriet_whitefield_1861';} or {$iri value 'eelizabeth_clamper_1760';} or" +
                "{$iri value 'ewilliam_lock_jacobs_1861';} or {$iri value 'ejames_jacobs_1806';} or" +
                "{$iri value 'eharriet_ann_young_1825';} or {$iri value 'ealec_john_archer_1927';} or" +
                "{$iri value 'eeleanor_pringle_1741';} or {$iri value 'ewilliam_rivers_lockey_1815';} or" +
                "{$iri value 'eviolet_heath_1887';} or {$iri value 'ejohn_archer_1835';} or" +
                "{$iri value 'ejeremiah_jacobs';} or {$iri value 'esarah_jewell_1790';} or" +
                "{$iri value 'eedward_young_1795';} or {$iri value 'eelizabeth_rivers_1787';} or" +
                "{$iri value 'ecatherine_thompson';} or {$iri value 'eelizabeth_gray_1810';} or" +
                "{$iri value 'ejohn_lockey_1789';} or {$iri value 'eeden_georgina_gardner_thompson_1810';} or" +
                "{$iri value 'ejames_whitfield_1821';} or {$iri value 'ewilliam_archer_1764';} or" +
                "{$iri value 'ejames_alexander_archer_1882';} or {$iri value 'epriscilla_saunders_1810';} or" +
                "{$iri value 'ehumphrey_archer_1726';} or {$iri value 'ejohn_archer_1804';} or" +
                "{$iri value 'ejames_whitfield_1792';} or {$iri value 'eann_norton_1799';} or" +
                "{$iri value 'ewilliam_lock';} or {$iri value 'esarah_lockey_1848';}; select $x;";
        assertEquals(inferRelationGrakn(isAncestorOfId, anneId), Sets.newHashSet(qb.<MatchQuery>parse(explicitAnneQuery)));

        String megaId = "mega_clamper_1995";
        String explicitMegaQuery = "match $x isa tPerson, has owl-iri $iri;" +
                "{$iri value 'esarah_rever_1850';} or {$iri value 'eelizabeth_frances_jessop_1869';} or" +
                "{$iri value 'epatricia_ann_kingswood_1944';} or {$iri value 'esarah_dickens_1801';} or" +
                "{$iri value 'ewilliam_rever_1870';} or {$iri value 'ejames_jessop_1836';} or" +
                "{$iri value 'eann_lodge_1763';} or {$iri value 'ewilliam_cotton';} or" +
                "{$iri value 'eedward_jessop_1802';} or {$iri value 'emartha_wife_of_john_cotton';} or" +
                "{$iri value 'efrances_spikin_1779';} or {$iri value 'erose_evlyn_rever_1906';} or" +
                "{$iri value 'eedward_blanchard_1771';} or {$iri value 'ejohn_jessop_1773';} or" +
                "{$iri value 'evincent_cotton_1808';} or {$iri value 'eamanda_usher_1968';} or" +
                "{$iri value 'esusanna_wife_of_william_cotton';} or {$iri value 'ejohn_cotton_1778';} or" +
                "{$iri value 'eelizabeth_blanchard_1807';} or {$iri value 'ejames_dickens_1774';} or" +
                "{$iri value 'emartha_cotton_1832';}; select $x;";
        assertEquals(inferRelationGrakn(isAncestorOfId, megaId), Sets.newHashSet(qb.<MatchQuery>parse(explicitMegaQuery)));
    }

    private void assertQueriesEqual(Stream<Map<String, Concept>> s1, Stream<Map<String, Concept>> s2) {
        assertEquals(s1.collect(Collectors.toSet()), s2.collect(Collectors.toSet()));
    }

    private QueryAnswers queryAnswers(MatchQuery query) {
        return new QueryAnswers(query.admin().streamWithVarNames().map(QueryAnswer::new).collect(toSet()));
    }
}
