package edu.indiana.cs.b649.hadoop.giraph;


import org.apache.giraph.Algorithm;
import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Demonstrates the basic Pregel triangle counting implementation.
 */
@Algorithm(
        name = "Shortest distance",
        description = "Return the total number of triangles"
)
public class TriangleCounting extends BasicComputation<
        IntWritable, DoubleWritable, FloatWritable, IntWritable> {
    /** The shortest paths id */

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(TriangleCounting.class);
 
    @Override
    public void compute(Vertex<IntWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<IntWritable> messages) throws IOException {
        /** First Superstep releases messages to vertexIds() whose value is greater than its value. Both VertexId and Message are double **/
        if (getSuperstep() == 0) {
            for (Edge<IntWritable, FloatWritable> edge: vertex.getEdges()) {
                if (edge.getTargetVertexId().compareTo(vertex.getId()) == 1) {
                    sendMessage(edge.getTargetVertexId(), vertex.getId());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Vertex " + vertex.getId() + " sent message " + vertex.getId() + " to vertex " + edge.getTargetVertexId());
                    }
                }
            }
        }

        /** Second superstep releases messages to message.get() < vertex.getId() < targetVertexId() **/
        if (getSuperstep() == 1) {
            for (IntWritable message: messages) {
                for (Edge<IntWritable, FloatWritable> edge: vertex.getEdges()) {
                    if (edge.getTargetVertexId().compareTo(vertex.getId()) + vertex.getId().compareTo(message) == 2) {
                        sendMessage(edge.getTargetVertexId(), message);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Vertex " + vertex.getId() + " sent message " + message + " to vertex " + edge.getTargetVertexId());
                        }
                    }
                }
            }
       }
       /** Sends messages to all its neighbours, the messages it receives **/
       if (getSuperstep() == 2) {
           for (IntWritable message: messages) {
               sendMessageToAllEdges(vertex, message);
           }
       }

       if (getSuperstep() == 3) {
           double Value = 0.0;
           for (IntWritable message: messages) {
               if (vertex.getId().equals(message)) {
                   Value += 1.0;
               }
           }
           vertex.setValue(new DoubleWritable(Value));
       }

       vertex.voteToHalt();
    }

    public static void main(String[] args) throws Exception {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        GiraphConfiguration conf = new GiraphConfiguration();
        GiraphRunner runner = new GiraphRunner();
        runner.setConf(conf);
        System.exit(ToolRunner.run(runner, new String[]{
                TriangleCounting.class.getName(),
                "-vip", "src/main/resources/giraph/triangle_input.txt",
                "-vif", "edu.indiana.cs.b649.hadoop.giraph.JsonIntDoubleFloatIntVertexInputFormat",
                "-vof", "edu.indiana.cs.b649.hadoop.giraph.VertexWithNonZeroValueOutputFormat",
                "-op", "src/main/resources/giraph/triangle_output/"+dateFormat.format(new Date()),
                "-w", "1",
                "-ca", "mapred.job.tracker=local",
                "-ca", "giraph.SplitMasterWorker=false",
                "-ca", "giraph.useSuperstepCounters=false"}));
                /* Two other useful properties when writing complex algorithms */
                //"-ca", "giraph.masterComputeClass=YourMasterClass",
                //"-ca", "giraph.workerContextClass=YourWorkerContextClass",


    }
}
