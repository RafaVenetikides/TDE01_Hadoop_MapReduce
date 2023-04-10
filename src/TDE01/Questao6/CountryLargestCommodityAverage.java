package TDE01.Questao6;

import TDE01.BrazilTransactions;
import advanced.entropy.BaseQtdWritable;
import advanced.entropy.EntropyFASTA;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
            InterruptedException{
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
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);
        FileInputFormat.addInputPath(j2, input);
        FileOutputFormat.setOutputPath(j2, output);
        FileOutputFormat.setOutputPath(j1, output);
        j1.waitForCompletion(false);
        j2.waitForCompletion(false);

    }
    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws  IOException, InterruptedException{

            String linha = value.toString();

            String colunas[] = linha.split(";");

            String chave = colunas[0];
            int qtd = 1;

            if (chave.equals("Brazil")){
                con.write(new Text(chave), new IntWritable(qtd));
            }

        }
    }



    public static class ReduceEtapaA extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException{

            int contagem = 0;
            for(IntWritable v : values){
                contagem += v.get();
            }
            con.write(key, new IntWritable(contagem));

        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws  IOException, InterruptedException{

            String linha = value.toString();

            String colunas[] = linha.split(";");

            String chave = colunas[0];
            int qtd = 1;

            if (chave.equals("Brazil")){
                con.write(new Text(chave), new IntWritable(qtd));
            }

        }
    }



    public static class ReduceEtapaB extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException{

            int contagem = 0;
            for(IntWritable v : values){
                contagem += v.get();
            }
            con.write(key, new IntWritable(contagem));

        }
    }
}
