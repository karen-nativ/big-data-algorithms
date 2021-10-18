import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class F1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	protected void reduce(Text word, Iterable<IntWritable>values, Context context) throws IOException,InterruptedException
	{
		int count = 0;
		for(IntWritable val:values)
			count += val.get();
		context.write(word, new IntWritable(count));
	}
}
