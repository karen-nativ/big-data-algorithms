package edu.indiana.cs.b649.hadoop.giraph;
  
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Output format for vertices with a int as id, a double as value and
 * non-zero value output
 */
public class VertexWithNonZeroValueOutputFormat extends
    TextVertexOutputFormat<IntWritable, DoubleWritable, FloatWritable> {
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new VertexWithPositiveDoubleValueWriter();
  }


  public class VertexWithPositiveDoubleValueWriter extends TextVertexWriter {
    @Override
    public void writeVertex(
        Vertex<IntWritable, DoubleWritable, FloatWritable> vertex)
      throws IOException, InterruptedException {
      if (vertex.getValue().compareTo(new DoubleWritable(0.0)) > 0) {
        StringBuilder output = new StringBuilder();
        output.append(vertex.getId());
	output.append("\t");
	output.append(vertex.getValue());
        getRecordWriter().write(new Text(output.toString()), null);
      }
    }
  }
}
