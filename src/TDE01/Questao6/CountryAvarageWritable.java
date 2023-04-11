package TDE01.Questao6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CountryAvarageWritable
        implements WritableComparable<CountryAvarageWritable> {
    private String country;
    private float valor;

    public CountryAvarageWritable() {
    }

    public CountryAvarageWritable(String country, float valot) {
        this.country = country;
        this.valor = valot;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
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
        CountryAvarageWritable that = (CountryAvarageWritable) o;
        return Float.compare(that.valor, valor) == 0 && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, valor);
    }

    @Override
    public int compareTo(CountryAvarageWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeFloat(valor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        valor = dataInput.readFloat();
    }
}
