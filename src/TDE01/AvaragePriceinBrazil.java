package TDE01;

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

public class AvaragePriceinBrazil {
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
        j.setMapperClass(BrazilTransactions.MapforBrazil.class);
        j.setReducerClass(BrazilTransactions.ReduceforBrazil.class);


        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(BrazilCommoditiesWritable.class);
        //j.setOutputKeyClass();
        //j.setOutputValueClass();

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }
    public static class MapforBrazil extends Mapper<LongWritable, Text, Text, BrazilCommoditiesWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws  IOException, InterruptedException{
            String linha = value.toString();
            if(!linha.startsWith("country")) {
                String colunas[] = linha.split(";");
                int year = Integer.parseInt(colunas[1]);
                String type = colunas[7];
                String category = colunas[8];
                int qtd = 1;

                if (colunas[0] == "Brazil" && colunas[4] == "Export"){
                    con.write(new Text("Chave"), new BrazilCommoditiesWritable(year, type, category, qtd) );
                }
            }
        }
    }



    public static class ReduceforBrazil extends Reducer<Text, BrazilCommoditiesWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<BrazilCommoditiesWritable> values, Context con)
                throws IOException, InterruptedException{
            int somaTotal = 0;
            for (BrazilCommoditiesWritable v : values){
                somaTotal += v.getQtd();
            }
        }
    }
}