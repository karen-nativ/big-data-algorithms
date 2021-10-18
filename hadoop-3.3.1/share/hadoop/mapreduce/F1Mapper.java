import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;    
import org.apache.hadoop.mapred.Reporter; 


public class F1Mapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	private Text total = new Text("total");
    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
    {
        String words[] = value.toString().split(" ");
        for(String word : words)
            context.write(total, new IntWritable(1));
    }
}
