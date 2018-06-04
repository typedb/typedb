package strategy;

import java.util.Random;

//public class strategy.DiscreteGaussianPDF extends PDF {
public class DiscreteGaussianPDF {
    private Random rand;

    public DiscreteGaussianPDF(Random rand, Double mean, Double variance) {
        this.rand = rand;
    }

    public int next() {
        return 5;
    }
}