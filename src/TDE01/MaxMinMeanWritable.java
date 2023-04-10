package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxMinMeanWritable
        implements WritableComparable<MaxMinMeanWritable> {
    private String type;
    private int year;

    public MaxMinMeanWritable() {
    }

    public MaxMinMeanWritable(String type, int year) {
        this.type = type;
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaxMinMeanWritable that = (MaxMinMeanWritable) o;
        return year == that.year && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, year);
    }

    @Override
    public int compareTo(MaxMinMeanWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeInt(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = dataInput.readUTF();
        year = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "MaxMinMeanWritable{" +
                "type='" + type + '\'' +
                ", year=" + year +
                '}';
    }
}
