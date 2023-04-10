package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxMinMeanResultsWritable
        implements WritableComparable<MaxMinMeanResultsWritable> {
    private Float maxPrice;
    private Float minPrice;
    private Float meanPrice;
    public MaxMinMeanResultsWritable() {
    }

    public MaxMinMeanResultsWritable(Float maxPrice, Float minPrice, Float meanPrice) {
        this.maxPrice = maxPrice;
        this.minPrice = minPrice;
        this.meanPrice = meanPrice;
    }

    public Float getMaxPrice() {
        return maxPrice;
    }

    public void setMaxPrice(Float maxPrice) {
        this.maxPrice = maxPrice;
    }

    public Float getMinPrice() {
        return minPrice;
    }

    public void setMinPrice(Float minPrice) {
        this.minPrice = minPrice;
    }

    public Float getMeanPrice() {
        return meanPrice;
    }

    public void setMeanPrice(Float meanPrice) {
        this.meanPrice = meanPrice;
    }

    @Override
    public String toString() {
        return "MaxMinMeanResultsWritable{" +
                "maxPrice=" + maxPrice +
                ", minPrice=" + minPrice +
                ", meanPrice=" + meanPrice +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanResultsWritable that = (MaxMinMeanResultsWritable) o;
        return Objects.equals(maxPrice, that.maxPrice) && Objects.equals(minPrice, that.minPrice) && Objects.equals(meanPrice, that.meanPrice);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPrice, minPrice, meanPrice);
    }

    @Override
    public int compareTo(MaxMinMeanResultsWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(maxPrice);
        dataOutput.writeFloat(minPrice);
        dataOutput.writeFloat(meanPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        maxPrice = dataInput.readFloat();
        minPrice = dataInput.readFloat();
        meanPrice = dataInput.readFloat();
    }
}
