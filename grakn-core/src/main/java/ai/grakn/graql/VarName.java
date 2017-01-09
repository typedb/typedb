package ai.grakn.graql;

import java.util.function.Function;

/**
 * A variable name in a Graql query
 */
public interface VarName {

    /**
     * Get the string name of the variable (without prefixed "$")
     */
    String getValue();

    /**
     * Rename a variable (does not modify the original {@code VarName})
     * @param mapper a function to apply to the underlying variable name
     * @return the new variable name
     */
    VarName map(Function<String, String> mapper);

    /**
     * Get a shorter representation of the variable (with prefixed "$")
     */
    String shortName();
}
