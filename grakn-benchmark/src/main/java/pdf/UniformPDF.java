package pdf;

import java.util.Random;
import java.util.stream.IntStream;

public class UniformPDF extends PDF {

    private Random rand;
    private int lowerBound;
    private int upperBound;

    public UniformPDF(Random rand, int lowerBound, int upperBound) {
        this.rand = rand;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public int next() {
        IntStream intStream = rand.ints(1, lowerBound, upperBound + 1);
        return intStream.findFirst().getAsInt();
    }
}
