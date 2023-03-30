package basic;

import java.io.IOException;

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


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/bible.txt");

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // Registro de classes
        j.setJarByClass(WordCount.class); // classe que contem o metodo MAIN
        j.setMapperClass(MapForWordCount.class); // classe que contem o metodo MAP
        j.setReducerClass(ReduceForWordCount.class); // classe que contem o metodo REDUCE
        j.setCombinerClass(CombineForWordCount.class); // classe que contem o combiner
        // nesse caso, reduce == combiner, entao podemos fazer direto:
        //j.setCombinerClass(ReduceForWordCount.class);

        // Definir os tipos de saida
        j.setMapOutputKeyClass(Text.class); // tipo da chave de saida do MAP
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saida do MAP
        j.setOutputKeyClass(Text.class); // tipo da chave de saida do REDUCE
        j.setOutputValueClass(IntWritable.class); // tipo do valor de saida do REDUCE

        // Definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }


    // 4 tipos do map:
    // 1o tipo: tipo da chave de entrada (long) => offset (irrelevante)
    // 2o tipo: tipo do valor de entrada (text) => uma linha do arquivo original
    // 3o tipo: tipo da chave de saida (text) => a palavra a ser contabilizada
    // 4o tipo: tipo do valor de saida (int) => numero 1
    //                                          (significa que encontramos a palavra 1 vez)
    public static class MapForWordCount extends Mapper<LongWritable, Text,
                                                        Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Convertendo a linha de entrada em uma string
            String linha = value.toString();

            // quebrando a linha em palavras
            String palavras[] = linha.split(" ");

            // iterando pelas palavras e criando (chave,valor)
            for(String p : palavras){
                Text chave = new Text(p);
                IntWritable valor = new IntWritable(1);

                // mandando a (chave,valor) pro reduce
                con.write(chave, valor);
            }


        }
    }

    // executado por Mapper!
    public static class CombineForWordCount extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // criando variavel de contagem
            int contagem = 0;

            // varrendo a lista e incrementando a contagem
            for (IntWritable v : values){
                contagem += v.get();
            }

            // salvando os resultados em disco
            con.write(key, new IntWritable(contagem));

        }
    }

    // 1o tipo: tipo da chave de entrada (text) => palavra
    // 2o tipo: tipo do valor de entrada (int) => ocorrencia = 1
    // 3o tipo: tipo da chave de saida (text) => palavra
    // 4o tipo: tipo do valor de saida (int) => contagem
    public static class ReduceForWordCount extends Reducer<Text, IntWritable,
                                                           Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // criando variavel de contagem
            int contagem = 0;

            // varrendo a lista e incrementando a contagem
            for (IntWritable v : values){
                contagem += v.get();
            }

            // salvando os resultados em disco
            con.write(key, new IntWritable(contagem));

        }
    }

}
