package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BrazilCommoditiesAverageVariables
        implements WritableComparable<BrazilCommoditiesAverageVariables> {
    private int qtd;
    private float valor;

    public BrazilCommoditiesAverageVariables() {
    }

    public BrazilCommoditiesAverageVariables(int qtd, float valor) {
        this.qtd = qtd;
        this.valor = valor;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    public float getValor() {
        return valor;
    }

    public void setValor(float valor) {
        this.valor = valor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrazilCommoditiesAverageVariables that = (BrazilCommoditiesAverageVariables) o;
        return qtd == that.qtd && valor == that.valor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(qtd, valor);
    }

    @Override
    public int compareTo(BrazilCommoditiesAverageVariables o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(qtd);
        dataOutput.writeFloat(valor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        qtd = dataInput.readInt();
        valor = dataInput.readFloat();
    }
}
