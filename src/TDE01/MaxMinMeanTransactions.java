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
        j.setMapperClass();
        j.setReducerClass();
    }
    public static class MapMaxMinMean extends Mapper<>{  // Setar valores de entrada e saida
        public void map(LongWritable key, Text value, Context con)
            throws IOException, InterruptedException{

        }
    }
    public  static class ReduceMaxMinMean extends Reducer<>{
        public void reduce()
            throws IOException, InterruptedException{

        }
    }
}
