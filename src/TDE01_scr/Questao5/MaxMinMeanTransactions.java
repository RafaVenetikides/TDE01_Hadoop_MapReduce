package TDE01.Questao5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class MaxMinMeanTransactions {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "MaxMinTransactions");

        j.setJarByClass(MaxMinMeanTransactions.class);
        j.setMapperClass(MapMaxMinMean.class);
        j.setCombinerClass(CombineMaxMinMean.class);
        j.setReducerClass(ReduceMaxMinMean.class);

        j.setMapOutputKeyClass(MaxMinMeanWritable.class);
        j.setMapOutputValueClass(MaxMinMeanValuesWritable.class);
        j.setOutputKeyClass(MaxMinMeanWritable.class);
        j.setOutputValueClass(MaxMinMeanResultsWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(false);
    }
    public static class MapMaxMinMean extends Mapper<LongWritable, Text, MaxMinMeanWritable, MaxMinMeanValuesWritable>{  // Setar valores de entrada e saida
        public void map(LongWritable key, Text value, Context con)
            throws IOException, InterruptedException{
            String linha = value.toString();
            if (!linha.startsWith("country")){
                String colunas[] = linha.split(";");
                String type = colunas[7];
                int year = Integer.parseInt(colunas[1]);
                float price = Float.parseFloat(colunas[5]);
                int qtd = 1;
                con.write(new MaxMinMeanWritable(type, year), new MaxMinMeanValuesWritable(price, qtd));
            }
        }
    }

    public static class CombineMaxMinMean extends Reducer<MaxMinMeanWritable, MaxMinMeanValuesWritable, MaxMinMeanWritable, MaxMinMeanValuesWritable>{
        public void reduce(MaxMinMeanWritable key, Iterable<MaxMinMeanValuesWritable> values, Context con)
                throws IOException, InterruptedException {

            float sumPrice = 0;
            int somaTotal = 0;

            for(MaxMinMeanValuesWritable o : values){
                sumPrice += o.getPrice();
                somaTotal += o.getQtd();
            }

            con.write(key, new MaxMinMeanValuesWritable(sumPrice, somaTotal));
        }
    }
    public  static class ReduceMaxMinMean extends Reducer<MaxMinMeanWritable, MaxMinMeanValuesWritable, MaxMinMeanWritable, MaxMinMeanResultsWritable>{
        public void reduce(MaxMinMeanWritable key, Iterable<MaxMinMeanValuesWritable> values, Context con)
            throws IOException, InterruptedException{
            float minPrice = Float.MAX_VALUE;
            float maxPrice = Float.MIN_VALUE;
            float sumPrice = 0;
            int somaTotal = 0;

            for (MaxMinMeanValuesWritable v : values){
                if (v.getPrice() > maxPrice){
                    maxPrice = v.getPrice();
                }
                if (v.getPrice() < minPrice){
                    minPrice = v.getPrice();
                }
                sumPrice += v.getPrice();
                somaTotal += v.getQtd();
            }

            float media = sumPrice/somaTotal;

            con.write(key, new MaxMinMeanResultsWritable(maxPrice, minPrice, media));
        }
    }
}
