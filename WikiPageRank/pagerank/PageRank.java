package pagerank;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRank extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
	String tmp=args[1]+"/tmp";
	boolean isCompleted = linkGraph(args[0], tmp+"00");
        if (!isCompleted) return 1;
	String output = "";

        for (int i = 0; i < 10; i++) {
            String input = tmp+ nf.format(i);
            output = tmp + nf.format(i + 1);

            isCompleted = runRankCalculation(input, output);

            if (!isCompleted) return 1;
        }
	    isCompleted = runSorter(output, args[1]+"/result");

       		 if (!isCompleted) return 1;
        return 0;
    }


    public boolean linkGraph(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job link = Job.getInstance(conf, "link");
        link.setJarByClass(PageRank.class);

        // Input / Mapper
        FileInputFormat.addInputPath(link, new Path(inputPath));
        link.setMapperClass(LinkGraphMapper.class);
        link.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(link, new Path(outputPath));
        link.setOutputFormatClass(TextOutputFormat.class);

        link.setOutputKeyClass(Text.class);
        link.setOutputValueClass(Text.class);
        link.setReducerClass(LinkGraphReducer.class);

        return link.waitForCompletion(true);
    }
 private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(PageRank.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(PageRankCalculateMapper.class);
        rankCalculator.setReducerClass(PageRankCalculateReducer.class);

        return rankCalculator.waitForCompletion(true);
    }
 private boolean runSorter(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job sortOrdering = Job.getInstance(conf, "sortOrdering");
        sortOrdering.setJarByClass(PageRank.class);

        sortOrdering.setOutputKeyClass(Text.class);
        sortOrdering.setOutputValueClass(Text.class);

        sortOrdering.setMapperClass(SortMapper.class);
	sortOrdering.setReducerClass(SortReducer.class);

        FileInputFormat.setInputPaths(sortOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(sortOrdering, new Path(outputPath));

        sortOrdering.setInputFormatClass(TextInputFormat.class);
        sortOrdering.setOutputFormatClass(TextOutputFormat.class);

        return sortOrdering.waitForCompletion(true);
    }

}
