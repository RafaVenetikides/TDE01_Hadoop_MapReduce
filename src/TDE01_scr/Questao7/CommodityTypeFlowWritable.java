package TDE01_scr.Questao7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityTypeFlowWritable
        implements WritableComparable<CommodityTypeFlowWritable> {
    private int commodity;
    private String flow;

    public CommodityTypeFlowWritable() {
    }

    public CommodityTypeFlowWritable(int commodity, String flow) {
        this.commodity = commodity;
        this.flow = flow;
    }

    public int getCommodity() {
        return commodity;
    }

    public void setCommodity(int commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public String toString() {
        return "CommodityTypeFlowWritable{" +
                "commodity='" + commodity + '\'' +
                ", flow=\t" + flow +
                "\t}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityTypeFlowWritable that = (CommodityTypeFlowWritable) o;
        return commodity == that.commodity && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, flow);
    }

    @Override
    public int compareTo(CommodityTypeFlowWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(commodity);
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readInt();
        flow = dataInput.readUTF();
    }
}
