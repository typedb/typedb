package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationshipAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtils;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.ConcurrentGraknServer;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class AtomicRuleApplicabilityIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    private static EmbeddedGraknSession ruleApplicabilitySession;
    private static EmbeddedGraknSession resourceApplicabilitySession;
    private static EmbeddedGraknSession reifiedResourceApplicabilitySession;

    private static void loadFromFile(String fileName, GraknSession session){
        try {
            InputStream inputStream = AtomicRuleApplicabilityIT.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            GraknTx tx = session.transaction(GraknTxType.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void loadContext(){
        ruleApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFile("ruleApplicabilityTest.gql", ruleApplicabilitySession);
        resourceApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFile("resourceApplicabilityTest.gql", resourceApplicabilitySession);
        reifiedResourceApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFile("reifiedResourceApplicabilityTest.gql", reifiedResourceApplicabilitySession);
    }

    @AfterClass
    public static void closeSession(){
        ruleApplicabilitySession.close();
        resourceApplicabilitySession.close();
        reifiedResourceApplicabilitySession.close();
    }
    

    @Test
    public void testRuleApplicability_OntologicalAtomsDoNotMatchAnyRules(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        Atom subAtom = ReasonerQueries.atomic(conjunction("{$x sub relationship;}", tx), tx).getAtom();
        Atom hasAtom = ReasonerQueries.atomic(conjunction("{$x has description;}", tx), tx).getAtom();
        Atom relatesAtom = ReasonerQueries.atomic(conjunction("{reifiable-relation relates $x;}", tx), tx).getAtom();
        Atom relatesAtom2 = ReasonerQueries.atomic(conjunction("{$x relates someRole;}", tx), tx).getAtom();
        Atom playsAtom = ReasonerQueries.atomic(conjunction("{$x plays someRole;}", tx), tx).getAtom();
        assertThat(subAtom.getApplicableRules().collect(toSet()), empty());
        assertThat(hasAtom.getApplicableRules().collect(toSet()), empty());
        assertThat(relatesAtom.getApplicableRules().collect(toSet()), empty());
        assertThat(relatesAtom2.getApplicableRules().collect(toSet()), empty());
        assertThat(playsAtom.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity; $z isa twoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap((relation.getRoleVarMap())));
        assertEquals(5, relation.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping_RolePlayerTypeMismatch(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        assertEquals(2, relation.getApplicableRules().count());
        tx.close();
    }

    @Test //threeRoleEntity subs twoRoleEntity -> (role, role, role)
    public void testRuleApplicability_AmbiguousRoleMapping_TypeHierarchyEnablesExtraRule(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{($x, $y, $z);$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
        assertEquals(2, relation.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_attributeBoundRelationPlayers(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);

        //attribute mismatch with the rule
        String relationString = "{($x, $y) isa attributed-relation;$x has resource-string 'valueOne'; $y has resource-string 'someValue';}";

        //attributes overlap with the ones from rule
        String relationString2 = "{($x, $y) isa attributed-relation;$x has resource-string 'someValue'; $y has resource-string 'yetAnotherValue';}";

        //attributes overlap with the ones from rule
        String relationString3 = "{($x, $y) isa attributed-relation;$x has resource-string 'someValue'; $y has resource-string contains 'Value';}";

        //generic relation with attributes not matching the rule ones
        String relationString4 = "{($x, $y);$x has resource-string 'valueOne'; $y has resource-string 'valueTwo';}";

        Atom relation = ReasonerQueries.create(conjunction(relationString, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);
        Atom relation2 = ReasonerQueries.create(conjunction(relationString2, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);
        Atom relation3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);
        Atom relation4 = ReasonerQueries.create(conjunction(relationString4, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);
        Set<InferenceRule> rules = RuleUtils.getRules(tx).map(r -> new InferenceRule(r, tx)).collect(Collectors.toSet());

        assertEquals(rules.stream().filter(r -> r.getRule().label().equals(Label.of("attributed-relation-long-rule"))).collect(Collectors.toSet()), relation.getApplicableRules().collect(toSet()));
        assertEquals(rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(toSet()), relation2.getApplicableRules().collect(toSet()));
        assertEquals(rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(toSet()), relation3.getApplicableRules().collect(toSet()));
        assertEquals(RuleUtils.getRules(tx).count() - 1, relation4.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_nonSpecificattributeBoundRelationPlayers(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);

        //attributes satisfy the ones from rule
        String relationString = "{($x, $y) isa attributed-relation;$x has resource-long 1334; $y has resource-long 1607;}";

        //inequality attribute not satisfied
        String relationString2 = "{" +
                "($x, $y, $z) isa attributed-relation;" +
                "$x has resource-long -1410;" +
                "$y has resource-long 0;" +
                "$z has resource-long 1667;" +
                "}";

        //attributes with inequalities than have overlap with rule ones
        String relationString3 = "{" +
                "($x, $y) isa attributed-relation;" +
                "$x has resource-long > -1667;" +
                "$y has resource-long > 20;" +
                "$z has resource-long < 2000;" +
                "}";

        Atom relation = ReasonerQueries.create(conjunction(relationString, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);
        Atom relation2 = ReasonerQueries.create(conjunction(relationString2, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);
        Atom relation3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);

        Set<InferenceRule> rules = RuleUtils.getRules(tx).map(r -> new InferenceRule(r, tx)).collect(Collectors.toSet());

        assertEquals(rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(Collectors.toSet()), relation.getApplicableRules().collect(Collectors.toSet()));
        assertEquals(rules.stream().filter(r -> r.getRule().label().equals(Label.of("attributed-relation-string-rule"))).collect(Collectors.toSet()), relation2.getApplicableRules().collect(Collectors.toSet()));
        assertEquals(rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(Collectors.toSet()), relation3.getApplicableRules().collect(Collectors.toSet()));
        tx.close();
    }

    @Test
    public void testRuleApplicability_MissingRelationPlayers(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);

        //inferred relation (role {role2, role3} : $x, role {role1, role2} : $y)
        String relationString = "{($x, $y);$x isa twoRoleEntity; $y isa anotherTwoRoleEntity;}";

        //inferred relation: (role1: $x, role1: $y)
        String relationString2 = "{($x, $y);$x isa singleRoleEntity; $y isa singleRoleEntity;}";

        //inferred relation: (role1: $x, role {role1, role2}: $y)
        String relationString3 = "{($x, $y);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity;}";

        //inferred relation: (role1: $x, role {role1, role2, role3}: $y)
        String relationString4 = "{($x, $y);$x isa singleRoleEntity; $y isa threeRoleEntity;}";

        //inferred relation: (role {role2, role3}: $x, role {role2, role3}: $y)
        String relationString5 = "{($x, $y);$x isa twoRoleEntity; $y isa twoRoleEntity;}";

        //inferred relation: (role {role1, role2}: $x, role {role1, role2}: $y)
        String relationString6 = "{($x, $y);$x isa anotherTwoRoleEntity; $y isa anotherTwoRoleEntity;}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, tx), tx).getAtom();
        Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, tx), tx).getAtom();
        Atom relation6 = ReasonerQueries.atomic(conjunction(relationString6, tx), tx).getAtom();

        assertEquals(6, relation.getApplicableRules().count());
        assertThat(relation2.getApplicableRules().collect(toSet()), empty());
        assertEquals(6, relation3.getApplicableRules().count());
        assertEquals(3, relation4.getApplicableRules().count());
        assertThat(relation5.getApplicableRules().collect(toSet()), empty());
        assertEquals(4, relation6.getApplicableRules().count());
        tx.close();
    }

    @Test //should assign (role : $x, role1: $y, role: $z) which is compatible with 3 ternary rules
    public void testRuleApplicability_WithWildcard(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$y isa singleRoleEntity; $z isa twoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        assertEquals(5, relation.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_TypedResources(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{$x isa reifiable-relation; $x has description $d;}";
        String relationString2 = "{$x isa typed-relation; $x has description $d;}";
        String relationString3 = "{$x isa relationship; $x has description $d;}";
        Atom resource = ReasonerQueries.create(conjunction(relationString, tx), tx).getAtoms(ResourceAtom.class).findFirst().orElse(null);
        Atom resource2 = ReasonerQueries.create(conjunction(relationString2, tx), tx).getAtoms(ResourceAtom.class).findFirst().orElse(null);
        Atom resource3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(ResourceAtom.class).findFirst().orElse(null);
        assertEquals(2, resource.getApplicableRules().count());
        assertEquals(2, resource2.getApplicableRules().count());
        assertEquals(3, resource3.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_DerivedTypes(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String typeString = "{$x isa reifying-relation;}";
        String typeString2 = "{$x isa typed-relation;}";
        String typeString3 = "{$x isa description;}";
        String typeString4 = "{$x isa attribute;}";
        String typeString5 = "{$x isa relationship;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
        Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, tx), tx).getAtom();
        Atom type4 = ReasonerQueries.atomic(conjunction(typeString4, tx), tx).getAtom();
        Atom type5 = ReasonerQueries.atomic(conjunction(typeString5, tx), tx).getAtom();

        List<InferenceRule> rules = RuleUtils.getRules(tx).map(r -> new InferenceRule(r, tx)).collect(Collectors.toList());
        assertEquals(2, type.getApplicableRules().count());
        assertEquals(1, type2.getApplicableRules().count());
        assertEquals(3, type3.getApplicableRules().count());
        assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isResource()).count(), type4.getApplicableRules().count());
        assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isRelation()).count(), type5.getApplicableRules().count());
        tx.close();
    }

    @Test //should assign (role: $x, role: $y) which is compatible with all rules
    public void testRuleApplicability_genericRelation(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{($x, $y);}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        assertEquals(
                RuleUtils.getRules(tx).count(),
                relation.getApplicableRules().count()
        );
        tx.close();
    }

    @Test
    public void testRuleApplicability_genericType(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String typeString = "{$x isa $type;}";
        String typeString2 = "{$x isa $type;$x isa entity;}";
        String typeString3 = "{$x isa $type;$x isa relationship;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
        Atom type3 = ReasonerQueries.create(conjunction(typeString3, tx), tx).getAtoms(IsaAtom.class).filter(at -> at.getSchemaConcept() == null).findFirst().orElse(null);

        assertEquals(RuleUtils.getRules(tx).count(), type.getApplicableRules().count());
        assertThat(type2.getApplicableRules().collect(toSet()), empty());
        assertEquals(RuleUtils.getRules(tx).filter(r -> r.thenTypes().allMatch(Concept::isRelationshipType)).count(), type3.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_genericTypeAsARoleplayer(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String typeString = "{(symmetricRole: $x); $x isa $type;}";
        String typeString2 = "{(someRole: $x); $x isa $type;}";
        Atom type = ReasonerQueries.create(conjunction(typeString, tx), tx).getAtoms(IsaAtom.class).findFirst().orElse(null);
        Atom type2 = ReasonerQueries.create(conjunction(typeString2, tx), tx).getAtoms(IsaAtom.class).findFirst().orElse(null);
        assertThat(type.getApplicableRules().collect(toSet()), empty());
        assertEquals(
                RuleUtils.getRules(tx).filter(r -> r.thenTypes().anyMatch(c -> c.label().equals(Label.of("binary")))).count(),
                type2.getApplicableRules().count()
        );
        tx.close();
    }

    @Test
    public void testRuleApplicability_genericTypeWithBounds(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{$x isa $type;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        assertEquals(
                RuleUtils.getRules(tx).count(),
                relation.getApplicableRules().count()
        );
        tx.close();
    }

    @Test
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
        String relationString = "{($x, $y, $z);$y isa singleRoleEntity; $z isa singleRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Var> roleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test //NB: role2 sub role1
    public void testRuleApplicability_RepeatingRoleTypesWithHierarchy(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{(someRole: $x1, someRole: $x2, subRole: $x3);}";
        String relationString2 = "{(someRole: $x1, subRole: $x2, subRole: $x3);}";
        String relationString3 = "{(subRole: $x1, subRole: $x2, subRole: $x3);}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
        assertEquals(1, relation.getApplicableRules().count());
        assertEquals(1, relation2.getApplicableRules().count());
        assertThat(relation3.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_genericRelationWithGenericType(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{($x, $y);$x isa noRoleEntity;}";
        String relationString2 = "{($x, $y);$x isa entity;}";
        String relationString3 = "{($x, $y);$x isa relationship;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
        Atom relation3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(RelationshipAtom.class).findFirst().orElse(null);

        assertEquals(7, relation.getApplicableRules().count());
        assertEquals(RuleUtils.getRules(tx).filter(r -> r.thenTypes().allMatch(Concept::isRelationshipType)).count(), relation2.getApplicableRules().count());

        //TODO not filtered correctly
        //assertEquals(RuleUtils.getRules(tx).filter(r -> r.thenTypes().allMatch(Concept::isAttributeType)).count(), relation3.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{(someRole: $x, subRole: $y) isa reifying-relation;}";
        String relationString2 = "{$x isa entity;(someRole: $x, subRole: $y) isa reifying-relation;}";
        String relationString3 = "{$x isa anotherTwoRoleEntity;(someRole: $x, subRole: $y) isa reifying-relation;}";
        String relationString4 = "{$x isa twoRoleEntity;(someRole: $x, subRole: $y) isa reifying-relation;}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, tx), tx).getAtom();
        assertEquals(2, relation.getApplicableRules().count());
        assertEquals(2, relation2.getApplicableRules().count());
        assertEquals(1, relation3.getApplicableRules().count());
        assertThat(relation4.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_TypePlayabilityDeterminesApplicability(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String relationString = "{$y isa singleRoleEntity;(someRole:$x, role:$y, anotherRole: $z) isa ternary;}";
        String relationString2 = "{$y isa twoRoleEntity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary;}";
        String relationString3 = "{$y isa anotherTwoRoleEntity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary;}";
        String relationString4 = "{$y isa noRoleEntity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary;}";
        String relationString5 = "{$y isa entity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary;}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
        Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
        Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, tx), tx).getAtom();
        Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, tx), tx).getAtom();

        assertEquals(1, relation.getApplicableRules().count());
        assertThat(relation2.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, relation3.getApplicableRules().count());
        assertEquals(1, relation4.getApplicableRules().count());
        assertEquals(1, relation5.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_TypeRelation(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String typeString = "{$x isa reifying-relation;}";
        String typeString2 = "{$x isa ternary;}";
        String typeString3 = "{$x isa binary;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
        Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, tx), tx).getAtom();
        assertEquals(2, type.getApplicableRules().count());
        assertEquals(2, type2.getApplicableRules().count());
        assertEquals(1, type3.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_OntologicalTypes(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String typeString = "{$x sub " + Schema.MetaSchema.RELATIONSHIP.getLabel() + ";}";
        String typeString2 = "{$x relates someRole;}";
        String typeString3 = "{$x plays someRole;}";
        String typeString4 = "{$x has name;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
        Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
        Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, tx), tx).getAtom();
        Atom type4 = ReasonerQueries.atomic(conjunction(typeString4, tx), tx).getAtom();
        assertThat(type.getApplicableRules().collect(toSet()), empty());
        assertThat(type2.getApplicableRules().collect(toSet()), empty());
        assertThat(type3.getApplicableRules().collect(toSet()), empty());
        assertThat(type4.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        Concept concept = getConcept(tx, "name", "noRoleEntity");
        String relationString = "{" +
                "($x, $y) isa ternary;" +
                "$x id '" + concept.id().getValue() + "';" +
                "}";
        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes_NoRelationType(){
        EmbeddedGraknTx<?> tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        Concept concept = getConcept(tx, "name", "noRoleEntity");
        String relationString = "{" +
                "($x, $y);" +
                "$x id '" + concept.id().getValue() + "';" +
                "}";

        Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        assertThat(relation.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ResourceDouble(){
        EmbeddedGraknTx<?> tx = resourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String resourceString = "{$x has res-double > 3.0;}";
        String resourceString2 = "{$x has res-double > 4.0;}";
        String resourceString3 = "{$x has res-double < 3.0;}";
        String resourceString4 = "{$x has res-double < 4.0;}";
        String resourceString5 = "{$x has res-double >= 5.0;}";
        String resourceString6 = "{$x has res-double <= 5.0;}";
        String resourceString7 = "{$x has res-double == 3.14;}";
        String resourceString8 = "{$x has res-double !== 5.0;}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();
        Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, tx), tx).getAtom();
        Atom resource5 = ReasonerQueries.atomic(conjunction(resourceString5, tx), tx).getAtom();
        Atom resource6 = ReasonerQueries.atomic(conjunction(resourceString6, tx), tx).getAtom();
        Atom resource7 = ReasonerQueries.atomic(conjunction(resourceString7, tx), tx).getAtom();
        Atom resource8 = ReasonerQueries.atomic(conjunction(resourceString8, tx), tx).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource4.getApplicableRules().count());
        assertThat(resource5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource6.getApplicableRules().count());
        assertEquals(1, resource7.getApplicableRules().count());
        assertEquals(1, resource8.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ResourceLong(){
        EmbeddedGraknTx<?> tx = resourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String resourceString = "{$x has res-long > 100;}";
        String resourceString2 = "{$x has res-long > 150;}";
        String resourceString3 = "{$x has res-long < 100;}";
        String resourceString4 = "{$x has res-long < 200;}";
        String resourceString5 = "{$x has res-long >= 130;}";
        String resourceString6 = "{$x has res-long <= 130;}";
        String resourceString7 = "{$x has res-long == 123;}";
        String resourceString8 = "{$x has res-long !== 200;}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();
        Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, tx), tx).getAtom();
        Atom resource5 = ReasonerQueries.atomic(conjunction(resourceString5, tx), tx).getAtom();
        Atom resource6 = ReasonerQueries.atomic(conjunction(resourceString6, tx), tx).getAtom();
        Atom resource7 = ReasonerQueries.atomic(conjunction(resourceString7, tx), tx).getAtom();
        Atom resource8 = ReasonerQueries.atomic(conjunction(resourceString8, tx), tx).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource4.getApplicableRules().count());
        assertThat(resource5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource6.getApplicableRules().count());
        assertEquals(1, resource7.getApplicableRules().count());
        assertEquals(1, resource8.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ResourceString(){
        EmbeddedGraknTx<?> tx = resourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String resourceString = "{$x has res-string contains 'ing';}";
        String resourceString2 = "{$x has res-string 'test';}";
        String resourceString3 = "{$x has res-string /.*(fast|string).*/;}";
        String resourceString4 = "{$x has res-string /.*/;}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();
        Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, tx), tx).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, resource3.getApplicableRules().count());
        assertEquals(1, resource4.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ResourceBoolean(){
        EmbeddedGraknTx<?> tx = resourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String resourceString = "{$x has res-boolean 'true';}";
        String resourceString2 = "{$x has res-boolean 'false';}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_TypeResource(){
        EmbeddedGraknTx<?> tx = resourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String typeString = "{$x isa resource;}";
        Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
        assertEquals(1, type.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        EmbeddedGraknTx<?> tx = resourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String resourceString = "{$x isa entity1, has resource $r;}";
        String resourceString2 = "{$x isa entity2, has resource $r;}";
        String resourceString3 = "{$x isa entity2, has resource 'test';}";

        Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
        Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
        Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();

        assertEquals(1, resource.getApplicableRules().count());
        assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ReifiedResourceDouble(){
        EmbeddedGraknTx<?> tx = reifiedResourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String queryString = "{$x isa res-double > 3.0;($x, $y);}";
        String queryString2 = "{$x isa res-double > 4.0;($x, $y);}";
        String queryString3 = "{$x isa res-double < 3.0;($x, $y);}";
        String queryString4 = "{$x isa res-double < 4.0;($x, $y);}";
        String queryString5 = "{$x isa res-double >= 5.0;($x, $y);}";
        String queryString6 = "{$x isa res-double <= 5.0;($x, $y);}";
        String queryString7 = "{$x isa res-double == 3.14;($x, $y);}";
        String queryString8 = "{$x isa res-double !== 5.0;($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, tx), tx).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, tx), tx).getAtom();
        Atom atom5 = ReasonerQueries.atomic(conjunction(queryString5, tx), tx).getAtom();
        Atom atom6 = ReasonerQueries.atomic(conjunction(queryString6, tx), tx).getAtom();
        Atom atom7 = ReasonerQueries.atomic(conjunction(queryString7, tx), tx).getAtom();
        Atom atom8 = ReasonerQueries.atomic(conjunction(queryString8, tx), tx).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        assertThat(atom3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom4.getApplicableRules().count());
        assertThat(atom5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom6.getApplicableRules().count());
        assertEquals(1, atom7.getApplicableRules().count());
        assertEquals(1, atom8.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ReifiedResourceLong(){
        EmbeddedGraknTx<?> tx = reifiedResourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String queryString = "{$x isa res-long > 100;($x, $y);}";
        String queryString2 = "{$x isa res-long > 150;($x, $y);}";
        String queryString3 = "{$x isa res-long < 100;($x, $y);}";
        String queryString4 = "{$x isa res-long < 200;($x, $y);}";
        String queryString5 = "{$x isa res-long >= 130;($x, $y);}";
        String queryString6 = "{$x isa res-long <= 130;($x, $y);}";
        String queryString7 = "{$x isa res-long == 123;($x, $y);}";
        String queryString8 = "{$x isa res-long !== 200;($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, tx), tx).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, tx), tx).getAtom();
        Atom atom5 = ReasonerQueries.atomic(conjunction(queryString5, tx), tx).getAtom();
        Atom atom6 = ReasonerQueries.atomic(conjunction(queryString6, tx), tx).getAtom();
        Atom atom7 = ReasonerQueries.atomic(conjunction(queryString7, tx), tx).getAtom();
        Atom atom8 = ReasonerQueries.atomic(conjunction(queryString8, tx), tx).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        assertThat(atom3.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom4.getApplicableRules().count());
        assertThat(atom5.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom6.getApplicableRules().count());
        assertEquals(1, atom7.getApplicableRules().count());
        assertEquals(1, atom8.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ReifiedResourceString(){
        EmbeddedGraknTx<?> tx = reifiedResourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String queryString = "{$x isa res-string contains 'val';($x, $y);}";
        String queryString2 = "{$x isa res-string 'test';($x, $y);}";
        String queryString3 = "{$x isa res-string /.*(fast|value).*/;($x, $y);}";
        String queryString4 = "{$x isa res-string /.*/;($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();
        Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, tx), tx).getAtom();
        Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, tx), tx).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        assertEquals(1, atom3.getApplicableRules().count());
        assertEquals(1, atom4.getApplicableRules().count());
        tx.close();
    }

    @Test
    public void testRuleApplicability_ReifiedResourceBoolean(){
        EmbeddedGraknTx<?> tx = reifiedResourceApplicabilitySession.transaction(GraknTxType.WRITE);
        String queryString = "{$x isa res-boolean 'true';($x, $y);}";
        String queryString2 = "{$x isa res-boolean 'false';($x, $y);}";

        Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
        Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();

        assertEquals(1, atom.getApplicableRules().count());
        assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        tx.close();
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, EmbeddedGraknTx<?> tx){
        Set<VarPatternAdmin> vars = tx.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    private Multimap<Role, Var> roleSetMap(Multimap<Role, Var> roleVarMap) {
        Multimap<Role, Var> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Concept getConcept(EmbeddedGraknTx<?> graph, String typeName, Object val){
        return graph.graql().match(var("x").has(typeName, val).admin()).get("x")
                .stream().map(ans -> ans.get("x")).findAny().orElse(null);
    }
}