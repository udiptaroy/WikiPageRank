//Follow these steps to execute the code and generate the output
 >mkdir pagerank
 //Copy paste all the .java files under the pagerank folders
 >hadoop fs -mkdir /user/uroy/pagerank /user/uroy/pagerank/input
> hadoop fs -put <<filename>> /user/uroy/pagerank/input
//cd to the parent folder of pagerank(where .java files are kept)
>hadoop fs -rm -r /user/uroy/pagerank/output
>javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d pagerank_classes pagerank/*.java
>jar -cvf pagerank.jar -C pagerank_classes/ .
>hadoop jar pagerank.jar pagerank/PageRank /user/uroy/pagerank/input /user/uroy/pagerank/output
>hadoop fs -cat /user/uroy/pagerank/output/result/*>wiki_simple_output.txt
>head -n 100 wiki_simple_output.txt>wiki_result.txt


