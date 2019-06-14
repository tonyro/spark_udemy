import java.io.Serializable;

public class IntegerWithSquareRoot implements Serializable {
    private Integer originalNumber;
    private Double squareRoot;

    public IntegerWithSquareRoot(Integer originalNumber) {
        this.originalNumber = originalNumber;
        this.squareRoot = Math.sqrt(originalNumber);
    }

    public Integer getOriginalNumber() {
        return originalNumber;
    }

    public Double getSquareRoot() {
        return squareRoot;
    }

    public String toString() {
        return "{ number : " + originalNumber + "; square root: " + squareRoot + " }";
    }
}
