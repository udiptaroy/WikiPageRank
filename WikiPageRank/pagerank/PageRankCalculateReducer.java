package pagerank;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankCalculateReducer extends Reducer<Text, Text, Text, Text> {

    private static final float damping = 0.85F;

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingPage = false;
        String[] split;
        float sumPageRanks = 0;
        String links = "";
        String page_rank;
        
        // For each otherPage amd calculate pagerank and add
        for (Text value : values){
            page_rank = value.toString();
            
            if(page_rank.equals("!")) {
                isExistingPage = true;
                continue;
            }
            
            if(page_rank.startsWith("|")){
                links = "\t"+page_rank.substring(1);
                continue;
            }

            split = page_rank.split("\\t");
            
            float pageRank = Float.valueOf(split[1]);
            int countLinks = Integer.valueOf(split[2]);
            
            sumPageRanks += (pageRank/countLinks);
        }

        if(!isExistingPage) return;
        float rank = damping * sumPageRanks + (1-damping);

        context.write(page, new Text(rank + links));
    }
}
