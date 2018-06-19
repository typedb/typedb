package pdf;

public class ConstantPDF extends PDF {

    private int constant;

    public ConstantPDF(int constant) {
        this.constant = constant;
    }

    @Override
    public int next() {
        return this.constant;
    }
}
