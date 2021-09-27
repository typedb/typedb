package com.vaticle.typedb.core.traversal.optimiser;

public class Constraint {

    private final double lowerBound;
    private final double upperBound;
    private final String name;

    public Constraint(double lowerBound, double upperBound, String name) {

        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.name = name;
    }

    public void setCoefficient(IntVariable variable, double coeff) {

    }
}
