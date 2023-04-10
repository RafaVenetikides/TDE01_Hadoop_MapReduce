package TDE01.Questao4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BrazilCommoditiesTypeWritable
        implements WritableComparable<BrazilCommoditiesTypeWritable> {
    private int year;
    private String type;
    private String category;

    public BrazilCommoditiesTypeWritable() {
    }

    public BrazilCommoditiesTypeWritable(int year, String type, String category) {
        this.year = year;
        this.type = type;
        this.category = category;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public String toString() {
        return "BrazilCommoditiesTypeWritable{" +
                "year=" + year +
                ", type='" + type + '\'' +
                ", category='" + category + '\'' +
                '}';
    }

    @Override
    public int compareTo(BrazilCommoditiesTypeWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(category);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        type = dataInput.readUTF();
        category = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrazilCommoditiesTypeWritable that = (BrazilCommoditiesTypeWritable) o;
        return year == that.year && Objects.equals(type, that.type) && Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, type, category);
    }
}
