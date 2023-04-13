package TDE01_scr.Questao3;

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

public class CommodityValuePerYear {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "AverageCommoditiesValueperYear");
        j.setJarByClass(CommodityValuePerYear.class);
        j.setMapperClass(MapforCommValues.class);
        j.setCombinerClass(CombineforCommValues.class);
        j.setReducerClass(ReduceforCommValues.class);


        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommValuesWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);

    }
    public static class MapforCommValues extends Mapper<LongWritable, Text, Text, CommValuesWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws  IOException, InterruptedException{

            String linha = value.toString();
            if(!linha.startsWith("country")) {
                String colunas[] = linha.split(";");

                String ano = colunas[1];
                float valor = Float.parseFloat(colunas[5]);
                int qtd = 1;

                con.write(new Text(ano), new CommValuesWritable(valor, qtd));
            }
        }
    }

    public static class CombineforCommValues extends Reducer<Text, CommValuesWritable, Text, CommValuesWritable>{
        public void reduce(Text key, Iterable<CommValuesWritable> values, Context con)
                throws IOException, InterruptedException {

            float somaValores = 0;
            int somaQtds = 0;
            for(CommValuesWritable o : values){
                somaQtds += o.getQtd();
                somaValores += o.getSomaValores();
            }

            con.write(key, new CommValuesWritable(somaValores, somaQtds));
        }
    }

    public static class ReduceforCommValues extends Reducer<Text, CommValuesWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<CommValuesWritable> values, Context con)
                throws IOException, InterruptedException {

            float somaValores = 0;
            int somaQtds = 0;
            for (CommValuesWritable o : values){
                somaValores += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            float media = somaValores/somaQtds;

            con.write(key, new FloatWritable(media));

        }
    }
}
