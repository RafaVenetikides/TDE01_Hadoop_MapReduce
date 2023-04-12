package TDE01.Questao7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CompareCommoditiesWritable
        implements WritableComparable<CompareCommoditiesWritable> {
    private String commodity;
    private int qtd;

    public CompareCommoditiesWritable() {
    }

    public CompareCommoditiesWritable(String commodity, int qtd) {
        this.commodity = commodity;
        this.qtd = qtd;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
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
        CompareCommoditiesWritable that = (CompareCommoditiesWritable) o;
        return qtd == that.qtd && Objects.equals(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, qtd);
    }

    @Override
    public int compareTo(CompareCommoditiesWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        qtd = dataInput.readInt();
    }
}
