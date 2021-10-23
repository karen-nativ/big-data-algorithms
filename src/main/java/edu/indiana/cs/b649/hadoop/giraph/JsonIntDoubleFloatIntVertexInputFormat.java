package edu.indiana.cs.b649.hadoop.giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;


/**
 * Input format for vertices with an int as id, a double as value and
 * int messages
 */
public class JsonIntDoubleFloatIntVertexInputFormat extends
    TextVertexInputFormat<IntWritable, DoubleWritable, FloatWritable> {
  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new JsonIntDoubleFloatIntVertexReader();
  }


  class JsonIntDoubleFloatIntVertexReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {


    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected IntWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new IntWritable(jsonVertex.getInt(0));
    }

    @Override
    protected DoubleWritable getValue(JSONArray jsonVertex) throws
      JSONException, IOException {
      return new DoubleWritable(jsonVertex.getDouble(1));
    }

    @Override
    protected Iterable<Edge<IntWritable, FloatWritable>> getEdges(
        JSONArray jsonVertex) throws JSONException, IOException {
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
      List<Edge<IntWritable, FloatWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
        edges.add(EdgeFactory.create(new IntWritable(jsonEdge.getInt(0)),
            new FloatWritable((float) jsonEdge.getDouble(1))));
      }
      return edges;
    }


    @Override
    protected Vertex<IntWritable, DoubleWritable, FloatWritable>
    handleException(Text line, JSONArray jsonVertex, JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

  }
}

