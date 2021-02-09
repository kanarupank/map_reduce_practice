package ai.nika.avg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;


public class AverageCalculator {
    private final static String TAB = "\t";
    private final static String COMMA = ",";
    private final static String JOB_NAME = "average";
    private static int numberOfNodes = 1; //passed as argument (optional), default to 1 for this avg problem
    private static int adjustSize = 0; //passed as argument (optional), default to 1 for this avg problem

    public static void main(String[] args) throws Exception {


        JobConf conf = new JobConf(AverageCalculator.class);
        conf.setJobName(JOB_NAME);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        //optional arg to set the number of nodes
        if (args.length > 2 && args[2] != null) {
            conf.set("numberOfNodes", args[2]);
        }
        if (args.length > 3 && args[3] != null) {
            conf.set("adjustSize", args[3]);
        }
        // conf.setNumReduceTasks(numberOfNodes);

        System.out.println("Number of nodes is set to " + numberOfNodes);
        JobClient.runJob(conf);
    }


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

        private final Text textInput = new Text();
        private final LongWritable zeroKey = new LongWritable(0);


        @Override
        public void configure(JobConf job) {
            super.configure(job);
            numberOfNodes = Integer.valueOf(job.get("numberOfNodes", "1"));
            adjustSize = Integer.valueOf(job.get("numberOfNodes", "0"));
            System.out.println("Configurations loaded");
        }

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            Random random = new Random();


            final HashMap<Integer, LongWritable> writables = new HashMap<>();

            for (int i = 0; i < numberOfNodes; i++) { //better numberOfNodes be configured
                writables.put(i, new LongWritable(i));
            }


            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();

                //could reuse the same jar for all iterations with the below hacks and sticking to the format
                if (token.contains(TAB)) {
                    token = token.split(TAB)[1]; //to maintain value,size when mapping from second time onward iteration
                }

                if (!token.contains(COMMA)) {
                    token += COMMA + 1; // to maintain value,size when mapping for the first iteration
                }
                textInput.set(token);


//                output.collect(zeroKey, textInput);
                output.collect(writables.get(random.nextInt(numberOfNodes)), textInput);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            double sum = 0;
            int size = 0;

            while (values.hasNext()) {
                String[] strArr = values.next().toString().split(COMMA);
                size += Double.valueOf(strArr[1]) + adjustSize; //for some reason getting actual size + number of avg
                sum += Double.valueOf(strArr[0]) * Double.valueOf(strArr[1]);
            }

            double avg = sum / size;
            //avg = Math.round(avg);//round if required
            output.collect(key, new Text(avg + COMMA + size));
        }
    }
}