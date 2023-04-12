package TDE01.Questao7;


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

public class MostCommercializedCommodity {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException{
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Path intermediate = new Path("./output/intermediateCommodities.tmp");

        Job j1 = new Job(c, "MostComercializedCommodity");

        Job j2 = new Job(c, "CompareCommodities");

        j1.setJarByClass(MostCommercializedCommodity.class);
        j1.setMapperClass(MapCommodityQuantities.class);
        //j.setCombinerClass(CombineCommoditiesQuantities.class);
        j1.setReducerClass(ReduceCommoditiesQuantities.class);

        j2.setJarByClass(MostCommercializedCommodity.class);
        j2.setMapperClass(MapCompareCommodities.class);
        //j2.setCombinerClass();
        j2.setReducerClass(ReduceCompareCommodities.class);

        j1.setMapOutputKeyClass(CommodityTypeFlowWritable.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(CommodityTypeFlowWritable.class);
        j1.setOutputValueClass(IntWritable.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CompareCommoditiesWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        j1.waitForCompletion(false);
        j2.waitForCompletion(false);
    }
    public static class MapCommodityQuantities extends Mapper<LongWritable, Text, CommodityTypeFlowWritable, IntWritable> {  // Setar valores de entrada e saida
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException{
            String linha = value.toString();
            if (!linha.startsWith("country")){
                String colunas[] = linha.split(";");
                if(colunas[1].equals("2016")) {
                    int commodity = Integer.parseInt(colunas[2]);
                    String flow = colunas[4];
                    int qtd = 1;
                    con.write(new CommodityTypeFlowWritable(commodity, flow), new IntWritable(qtd));
                }
            }
        }
    }
    /*
    public static class CombineCommoditiesQuantities extends Reducer<, , , > {
        public void reduce( key, Iterable<> values, Context con)
                throws IOException, InterruptedException {



            con.write();
        }
    }
    */

    public  static class ReduceCommoditiesQuantities extends Reducer<CommodityTypeFlowWritable, IntWritable, CommodityTypeFlowWritable, IntWritable>{
        public void reduce(CommodityTypeFlowWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException{

            int somaQtds = 0;
            for (IntWritable v : values){
                somaQtds += v.get();
            }

            con.write(key, new IntWritable(somaQtds));
        }
    }

    public static class MapCompareCommodities extends Mapper<LongWritable, Text, Text, CompareCommoditiesWritable> {  // Setar valores de entrada e saida
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException{
            String linha = value.toString();

            String colunas[] = linha.split("\t");

            String commodity = colunas[0];
            int qtd = Integer.parseInt(colunas[1]);

            con.write(new Text("Chave"), new CompareCommoditiesWritable(commodity, qtd));
        }
    }
    /*
    public static class CombineCommoditiesQuantities extends Reducer<, , , > {
        public void reduce( key, Iterable<> values, Context con)
                throws IOException, InterruptedException {



            con.write();
        }
    }
    */

    public  static class ReduceCompareCommodities extends Reducer<Text, CompareCommoditiesWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<CompareCommoditiesWritable> values, Context con)
                throws IOException, InterruptedException{

            int maiorSoma = Integer.MIN_VALUE;
            String commodity = null;
            for(CompareCommoditiesWritable v : values){
                if (v.getQtd() > maiorSoma){
                    maiorSoma = v.getQtd();
                    commodity = v.getCommodity();
                }
            }
            con.write(new Text(commodity), new IntWritable(maiorSoma));
        }
    }
}
