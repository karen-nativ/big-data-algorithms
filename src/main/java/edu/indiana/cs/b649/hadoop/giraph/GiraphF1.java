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
import org.apache.hadoop.io.Text;
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
        name = "F1",
        description = "Calculates F1 frequency of given input"
)

public class GiraphF1 extends BasicComputation<
        Text, DoubleWritable, DoubleWritable, DoubleWritable> {
    /** The shortest paths id 
    public static final LongConfOption SOURCE_ID =
            new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
                    "The shortest paths id");
    */
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(GiraphF1.class);

    @Override
    public void compute(
            Vertex<Text, DoubleWritable, DoubleWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
	double total = 0d;
        for (DoubleWritable message : messages) {
            total = Math.max(total, message.get());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " got intermediate total = " + total +
                    " vertex value = " + vertex.getValue());
        }
        vertex.setValue(new DoubleWritable(total + 1));
	sendMessageToAllEdges(vertex, new DoubleWritable(total + 1));	
        vertex.voteToHalt();
    }

    public static void main(String[] args) throws Exception {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        GiraphConfiguration conf = new GiraphConfiguration();
        GiraphRunner runner = new GiraphRunner();
        runner.setConf(conf);
        System.exit(ToolRunner.run(runner, new String[]{
                GiraphF1.class.getName(),
                "-vip", "src/main/resources/giraph/input.txt",
                "-vif", "org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat",
                "-vof", "edu.indiana.cs.b649.hadoop.giraph.VertexWithSingleDoubleOutputFormat",
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
