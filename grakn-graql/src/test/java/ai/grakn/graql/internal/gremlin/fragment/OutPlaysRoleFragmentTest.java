package ai.grakn.graql.internal.gremlin.fragment;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Ctor;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class OutPlaysRoleFragmentTest {

    @Property
    public void testApplyTraversalDoesNotTraverseSubs(@From(Ctor.class) OutPlaysRoleFragment fragment) {
    }
}