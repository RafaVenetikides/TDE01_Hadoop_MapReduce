package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxMinMeanValuesWritable
        implements WritableComparable<MaxMinMeanValuesWritable> {
    private Float price;
    private int qtd;

    public MaxMinMeanValuesWritable() {
    }

    public MaxMinMeanValuesWritable(Float price, int qtd) {
        this.price = price;
        this.qtd = qtd;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanValuesWritable that = (MaxMinMeanValuesWritable) o;
        return qtd == that.qtd && Objects.equals(price, that.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, qtd);
    }

    @Override
    public int compareTo(MaxMinMeanValuesWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(price);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        price = dataInput.readFloat();
        qtd = dataInput.readInt();
    }
}
