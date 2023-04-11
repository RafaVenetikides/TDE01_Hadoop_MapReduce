package TDE01.Questao6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class CountryLargestCommodityAverage {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Path intermediate = new Path("./output/intermediate.tmp");

        Job j1 = new Job(c, "CommodityAverage");

        Job j2 = new Job(c, "CountryLargestAverage");

        j1.setJarByClass(CountryLargestCommodityAverage.class);
        j1.setMapperClass(CountryLargestCommodityAverage.MapEtapaA.class);
        j1.setReducerClass(CountryLargestCommodityAverage.ReduceEtapaA.class);

        j2.setJarByClass(CountryLargestCommodityAverage.class);
        j2.setMapperClass(CountryLargestCommodityAverage.MapEtapaB.class);
        j2.setReducerClass(CountryLargestCommodityAverage.ReduceEtapaB.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(CountryCommoditiesValuesWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(FloatWritable.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CountryAvarageWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);




        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        j1.waitForCompletion(false);
        j2.waitForCompletion(false);

    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, CountryCommoditiesValuesWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.startsWith("country")) {
                String colunas[] = linha.split(";");
                if (colunas[4].equals("Export")) {
                    String country = colunas[0];
                    float price = Float.parseFloat(colunas[5]);
                    int qtd = 1;

                    con.write(new Text(country), new CountryCommoditiesValuesWritable(price, qtd));
                }
            }
        }
    }


    public static class ReduceEtapaA extends Reducer<Text, CountryCommoditiesValuesWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<CountryCommoditiesValuesWritable> values, Context con)
                throws IOException, InterruptedException {

            int somaTotal = 0;
            float somaPrecos = 0;
            for (CountryCommoditiesValuesWritable v : values) {
                somaTotal += v.getQtd();
                somaPrecos += v.getValor();
            }

            float media = somaPrecos / somaTotal;

            con.write(key, new FloatWritable(media));

        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, CountryAvarageWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String campos [] = linha.split("\t");

            String country = campos[0];
            float average = Float.parseFloat(campos[1]);
            con.write(new Text("Chave"), new CountryAvarageWritable(country, average));
        }
    }


    public static class ReduceEtapaB extends Reducer<Text, CountryAvarageWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<CountryAvarageWritable> values, Context con)
                throws IOException, InterruptedException {
            String country = null;
            float valor = Float.MIN_VALUE;
            for(CountryAvarageWritable v : values){
                if(v.getValor() > valor){
                    country = v.getCountry();
                    valor = v.getValor();
                }
            }
            con.write(new Text(country), new FloatWritable(valor));
        }
    }
}

