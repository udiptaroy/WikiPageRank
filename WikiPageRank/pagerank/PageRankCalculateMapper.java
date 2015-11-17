package pagerank;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int page_pos = value.find("\t");
        int rank_pos = value.find("\t", page_pos+1);

        String page = Text.decode(value.getBytes(), 0, page_pos);
        String page_rank = Text.decode(value.getBytes(), 0, rank_pos+1);
        
        // ! to make it existing to replace dead nodes which have no out nodes
        context.write(new Text(page), new Text("!"));

        // Skip pages with no links.
        if(rank_pos == -1) return;
        
        String links = Text.decode(value.getBytes(), rank_pos+1, value.getLength()-(rank_pos+1));
        String[] validPages = links.split(",");
        int totalLinks = validPages.length;
        
        for (String otherPage : validPages){
            Text pageRankTotalLinks = new Text(page_rank + totalLinks);
            context.write(new Text(otherPage), pageRankTotalLinks);
        }
        
        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|" + links));
    }
}
