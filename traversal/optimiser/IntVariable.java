package com.vaticle.typedb.core.traversal.optimiser;

public class IntVariable {

    private final double lowerBound;
    private final double upperBound;
    private final String name;

    public IntVariable(double lowerBound, double upperBound, String name) {

        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.name = name;
    }

    public int solutionValue() {

    }

}
