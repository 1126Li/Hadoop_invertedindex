import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Inverted_Index {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text location = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            int position = 0;

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                word.set(token);
                location.set(fileName + "@" + position);
                context.write(word, location);
                position += token.length() + 1; // Adding 1 for space or other delimiter
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> docCountMap = new HashMap<>();

            for (Text value : values) {
                String location = value.toString();
                location = location.replaceAll("ir_hdfs@", ""); // Remove "ir_hdfs@" string
                docCountMap.put(location, docCountMap.getOrDefault(location, 0) + 1);
            }

            StringBuilder result = new StringBuilder();
            for (String location : docCountMap.keySet()) {
                result.append(location).append(":").append(docCountMap.get(location)).append(",");
            }
            if (result.length() > 0) {
                result.setLength(result.length() - 1);
            }

            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");

        job.setJarByClass(inverted_index.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
