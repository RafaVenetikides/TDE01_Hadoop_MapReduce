package TDE01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AveragePriceinBrazil {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "BrazilTransactions");

        j.setJarByClass(BrazilTransactions.class);
        j.setMapperClass(MapforBrazilCommodities.class);
        j.setReducerClass(ReduceforBrazilCommodities.class);

        j.setMapOutputKeyClass(BrazilCommoditiesTypeWritable.class);
        j.setMapOutputValueClass(BrazilCommoditiesAverageVariables.class);
        j.setOutputKeyClass(BrazilCommoditiesTypeWritable.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }
    public static class MapforBrazilCommodities extends Mapper<LongWritable, Text, BrazilCommoditiesTypeWritable, BrazilCommoditiesAverageVariables> {
        public void map(LongWritable key, Text value, Context con)
                throws  IOException, InterruptedException{
            String linha = value.toString();
            if(!linha.startsWith("country")) {
                String colunas[] = linha.split(";");
                int year = Integer.parseInt(colunas[1]);
                String type = colunas[7];
                String category = colunas[8];
                float price = Float.parseFloat(colunas[5]);
                int qtd = 1;

                if (colunas[0].equals("Brazil") && colunas[4].equals("Export")){
                    con.write(new BrazilCommoditiesTypeWritable(year, type, category), new BrazilCommoditiesAverageVariables(qtd, price));
                }
            }
        }
    }



    public static class ReduceforBrazilCommodities extends Reducer<BrazilCommoditiesTypeWritable, BrazilCommoditiesAverageVariables, BrazilCommoditiesTypeWritable, FloatWritable> {
        public void reduce(BrazilCommoditiesTypeWritable key, Iterable<BrazilCommoditiesAverageVariables> values, Context con)
                throws IOException, InterruptedException{
            int somaTotal = 0;
            float somaPrecos = 0;
            for (BrazilCommoditiesAverageVariables v : values){
                somaTotal += v.getQtd();
                somaPrecos += v.getValor();
            }
            float media = somaPrecos/somaTotal;
            con.write(key, new FloatWritable(media));

        }
    }
}