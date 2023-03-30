package TDE01;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommValuesWritable
        implements WritableComparable<CommValuesWritable>{
    private float somaValores;
    private int qtd;

    public CommValuesWritable() {
    }

    public CommValuesWritable(float somaValores, int qtd) {
        this.somaValores = somaValores;
        this.qtd = qtd;
    }

    public float getSomaValores() {
        return somaValores;
    }

    public void setSomaValores(float somaValores) {
        this.somaValores = somaValores;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(CommValuesWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(somaValores);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaValores = dataInput.readFloat();
        qtd = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommValuesWritable that = (CommValuesWritable) o;
        return Float.compare(that.somaValores, somaValores) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaValores, qtd);
    }
}
