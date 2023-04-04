package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BrazilCommoditiesWritable
        implements WritableComparable<BrazilCommoditiesWritable> {
    private int year;
    private String commoditieType;
    private String category;
    private int qtd;
    public BrazilCommoditiesWritable() {
    }

    public BrazilCommoditiesWritable(int year, String commoditieType, String category, int qtd) {
        this.year = year;
        this.commoditieType = commoditieType;
        this.category = category;
        this.qtd = qtd;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getCommoditieType() {
        return commoditieType;
    }

    public void setCommoditieType(String commoditieType) {
        this.commoditieType = commoditieType;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
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
        BrazilCommoditiesWritable that = (BrazilCommoditiesWritable) o;
        return year == that.year && qtd == that.qtd && Objects.equals(commoditieType, that.commoditieType) && Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, commoditieType, category, qtd);
    }

    @Override
    public int compareTo(BrazilCommoditiesWritable brazilCommoditiesWritable) {
        return Integer.compare(brazilCommoditiesWritable.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(commoditieType);
        dataOutput.writeUTF(category);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        commoditieType = dataInput.readUTF();
        category = dataInput.readUTF();
        qtd = dataInput.readInt();
    }
}
