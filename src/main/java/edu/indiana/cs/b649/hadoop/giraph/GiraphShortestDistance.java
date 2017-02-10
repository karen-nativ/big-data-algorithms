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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
        name = "Shortest distance",
        description = "Finds all shortest distances from a selected vertex"
)
public class GiraphShortestDistance extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /** The shortest paths id */
    public static final LongConfOption SOURCE_ID =
            new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
                    "The shortest paths id");
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(GiraphShortestDistance.class);

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
        }
        double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
                    " vertex value = " + vertex.getValue());
        }
        if (minDist < vertex.getValue().get()) {
            vertex.setValue(new DoubleWritable(minDist));
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                double distance = minDist + edge.getValue().get();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Vertex " + vertex.getId() + " sent to " +
                            edge.getTargetVertexId() + " = " + distance);
                }
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
            }
        }
        vertex.voteToHalt();
    }

    public static void main(String[] args) throws Exception {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        GiraphConfiguration conf = new GiraphConfiguration();
        GiraphRunner runner = new GiraphRunner();
        runner.setConf(conf);
        System.exit(ToolRunner.run(runner, new String[]{
                GiraphShortestDistance.class.getName(),
                "-vip", "src/main/resources/giraph/tiny_graph.txt",
                "-vif", "org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat",
                "-vof", "org.apache.giraph.io.formats.IdWithValueTextOutputFormat",
                "-op", "src/main/resources/giraph/output/"+dateFormat.format(new Date()),
                "-w", "1",
                "-ca", "mapred.job.tracker=local",
                "-ca", "giraph.SplitMasterWorker=false",
                "-ca", "giraph.useSuperstepCounters=false"}));
                /* Two other useful properties when writing complex algorithms */
                //"-ca", "giraph.masterComputeClass=YourMasterClass",
                //"-ca", "giraph.workerContextClass=YourWorkerContextClass",


    }
}
