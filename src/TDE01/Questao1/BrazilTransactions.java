package TDE01.Questao1;

import basic.WordCount;
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

public class BrazilTransactions {
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
        j.setMapperClass(MapforBrazil.class);
        j.setReducerClass(ReduceforBrazil.class);


        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }
    public static class MapforBrazil extends Mapper<LongWritable, Text, Text, IntWritable>{
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



    public static class ReduceforBrazil extends Reducer<Text, IntWritable, Text, IntWritable>{
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
