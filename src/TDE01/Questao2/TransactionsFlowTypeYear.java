package TDE01.Questao2;

import TDE01.Questao1.BrazilTransactions;
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

public class TransactionsFlowTypeYear {
    public static void main(String args[]) throws IOException,

            ClassNotFoundException,
            InterruptedException{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "TransactionsPerFlowTypeYear");

        j.setJarByClass(TransactionsFlowTypeYear.class);
        j.setMapperClass(MapforFlowtypeYear.class);
        j.setCombinerClass(CombineForAverage.class);
        j.setReducerClass(ReduceforFlowtypeYear.class);

        j.setMapOutputKeyClass(FlowTypeYearWritable.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(FlowTypeYearWritable.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }
    public static class MapforFlowtypeYear extends Mapper<LongWritable, Text, FlowTypeYearWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws  IOException, InterruptedException{

            String linha = value.toString();
            if(!linha.startsWith("country")) {
                String colunas[] = linha.split(";");

                String flowType = colunas[4];
                int year = Integer.parseInt(colunas[1]);
                int qtd = 1;

                con.write(new FlowTypeYearWritable(flowType, year), new IntWritable(qtd));
            }
        }
    }

    public static class CombineForAverage extends Reducer<Text, IntWritable, FlowTypeYearWritable, IntWritable>{
        public void reduce(FlowTypeYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar as temperaturas e as qtds para cada chave
            int somaQtds = 0;
            for(IntWritable o : values){
                somaQtds += o.get();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new IntWritable(somaQtds));


        }
    }

    public static class ReduceforFlowtypeYear extends Reducer<FlowTypeYearWritable, IntWritable, FlowTypeYearWritable, IntWritable> {
        public void reduce(FlowTypeYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException{

            int somaQtds = 0;

            for (IntWritable o : values) {
                somaQtds += o.get();
            }
            con.write(key, new IntWritable(somaQtds));
        }
    }
}
