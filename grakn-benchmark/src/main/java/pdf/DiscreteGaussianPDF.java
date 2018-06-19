package pdf;

import java.util.Random;

public class DiscreteGaussianPDF extends PDF {
    private Random rand;
    private Double mean;
    private Double variance;

    public DiscreteGaussianPDF(Random rand, Double mean, Double variance) {
        this.rand = rand;
        this.mean = mean;
        this.variance = variance;
    }

    public int next() {
        double z = rand.nextGaussian();
        return (int) (Math.pow(variance, 0.5) * z + mean);
    }
}