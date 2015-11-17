package pagerank;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Map;
import java.io.IOException;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Iterator;
public class SortReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	HashMap rank=new HashMap<String,Float>();
        for (Text value : values) {
        
	String[] pagerank=value.toString().split("#");
	rank.put(pagerank[1],Float.parseFloat(pagerank[0]));
        
	}
	 	Map<String, Float> result = sortByComparator(rank);

		printMap(result,context);
        
    }

	public static void printMap(Map<String,Float> map,Context context) throws IOException, InterruptedException  {
		for (Map.Entry<String,Float> entry : map.entrySet()) {
			context.write(new Text(entry.getKey()),new Text(entry.getValue()+""));
		}
	}

	private static Map<String, Float> sortByComparator(Map<String, Float> unsortMap) {

		// Convert Map to List
		List<Map.Entry<String, Float>> list = 
			new LinkedList<Map.Entry<String, Float>>(unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
			public int compare(Map.Entry<String, Float> o1,
                                           Map.Entry<String, Float> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		for (Iterator<Map.Entry<String, Float>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Float> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}





}
