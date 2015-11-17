package pagerank;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LinkGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = parseTitleAndText(value);
        
        String pageString = input[0];
      	 if(notValidPage(pageString))
            return;
        
        Text page = new Text(pageString.replace(' ', '_'));

        Matcher matcher = wikiLinksPattern.matcher(input[1]);
        
         while (matcher.find()) {
            String page2 = matcher.group();
            //Choose only wiki pages
            page2 = getWikiPageFromLink(page2);
            if(page2 == null || page2.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            context.write(page, new Text(page2));
        }
    }
    

    private String getWikiPageFromLink(String link){
	if(isNotWikiLink(link)) return null;
              
        int start = link.startsWith("[[") ? 2 : 1;
        int endLink = link.indexOf("]");
	
	int pipe = link.indexOf("|");
        if(pipe > 0){
            endLink = pipe;
        }
      
        link =  link.substring(start, endLink);
        link = link.replaceAll("\\s", "_");
        link = link.replaceAll(",", "");
              
        return link;
    }
    
 
    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

    private boolean isNotWikiLink(String link) {
        int start = 1;
        if(link.startsWith("[[")){
            start = 2;
        }
        
        if( link.length() < start+2 || link.length() > 100) return true;
        char first = link.charAt(start);
        
        if( first == '#') return true;
        if( first == ',') return true;
        if( first == '.') return true;
        if( first == '&') return true;
        if( first == '\'') return true;
        if( first == '-') return true;
        if( first == '{') return true;
        
        if( link.contains(":")) return true; 
        if( link.contains(",")) return true; 
        if( link.contains("&")) return true;
        
        return false;
    }
 private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }
}


