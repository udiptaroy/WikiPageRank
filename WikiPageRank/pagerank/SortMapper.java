package pagerank;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] page_rank = getPageAndRank(key, value);
        
        float parseFloat = Float.parseFloat(page_rank[1]);
        
        Text page = new Text(page_rank[0]);
        FloatWritable rank = new FloatWritable(parseFloat);

        context.write(new Text("1"),new Text(rank+"#"+page));
    }
    
    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
        String[] page_rank = new String[2];
        int page = value.find("\t");
        int rank = value.find("\t", page + 1);
        int end;
        if (rank == -1) {
            end = value.getLength() - (page + 1);
        } else {
            end = rank - (page + 1);
        }
        
        page_rank[0] = Text.decode(value.getBytes(), 0, page);
        page_rank[1] = Text.decode(value.getBytes(), page + 1, end);
        
        return page_rank;
    }
    
}
