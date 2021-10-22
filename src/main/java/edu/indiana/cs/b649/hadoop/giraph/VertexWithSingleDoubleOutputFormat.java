package edu.indiana.cs.b649.hadoop.giraph;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Output format for vertices with a text as id, a double as value and
 * single double output
 */
public class VertexWithSingleDoubleOutputFormat extends
    TextVertexOutputFormat<Text, DoubleWritable, DoubleWritable> {
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new VertexWithDoubleValueWriter();
  }

  /**
   * Vertex writer used with
   * {@link VertexWithSingleDoubleOutputFormat}.
   */
  public class VertexWithDoubleValueWriter extends TextVertexWriter {
    @Override
    public void writeVertex(
        Vertex<Text, DoubleWritable, DoubleWritable> vertex)
      throws IOException, InterruptedException {
      boolean is_last = true;
      for(Edge<Text, DoubleWritable> edge : vertex.getEdges()) {
            is_last = false;
      }
      if (is_last) {
      	StringBuilder output = new StringBuilder();
      	output.append(vertex.getValue());
      	getRecordWriter().write(new Text(output.toString()), null);
      }
    }
  }
}
