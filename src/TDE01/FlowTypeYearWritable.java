package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowTypeYearWritable
        implements WritableComparable<FlowTypeYearWritable>{

    private String flowType;
    private int year;

    public FlowTypeYearWritable() {
    }

    public FlowTypeYearWritable(String flowType, int year) {
        this.flowType = flowType;
        this.year = year;
    }

    public String getFlowType() {
        return flowType;
    }

    public void setFlowType(String flowType) {
        this.flowType = flowType;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public int compareTo(FlowTypeYearWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flowType);
        dataOutput.writeInt(year);
    }

    @Override
    public String toString() {
        return "FlowTypeYearWritable{" +
                "flowType='" + flowType + '\'' +
                ", year=" + year +
                '}';
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flowType = dataInput.readUTF();
        year = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowTypeYearWritable that = (FlowTypeYearWritable) o;
        return year == that.year && Objects.equals(flowType, that.flowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowType, year);
    }
}
