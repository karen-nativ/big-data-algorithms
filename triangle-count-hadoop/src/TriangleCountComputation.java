package org.computations;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.io.PrintStream;
import java.io.FileOutputStream;
import org.utilities.*;

public class TriangleCountComputation {

  public static class DegreeCountMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      //logInput("map", key, value);
      StringPair edge = new StringPair(key, ':', value);
      output.collect(key, edge.toText());
      output.collect(value, edge.toText());
    }
  }
  private static class DegreeCountReduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text edge_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int counter = 0;
      List<String> edges = new ArrayList<>();
      while(values.hasNext()){
        Text next_value = values.next();
        edges.add(new String(next_value.toString()));
        counter++;
      }
      String count = String.valueOf(counter);
    
      //logPrint("reduce 1: "+edge_key);
      for(String edge : edges){
        String value;
        if(edge.split(":")[0].equals(edge_key.toString())){
          value = new String(count + ":");
        } else {
          value = new String(":" + count);
        }
        //logPrint("\tgot: "+edge_key+", "+edge+"\t\t returning: "+edge+", "+value);
        output.collect(new Text(edge), new Text(value));
      }
    }
   }
  
  private static class DegreeCountReduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    public static String combineSides(String side_a, String side_b){
      //logPrint("combine sides got: " + side_a + ", " + side_b);
      String result;
      if(side_a.charAt(0) == ':'){
        result = side_b.split(":")[0] +":"+ side_a.split(":")[1];
      } else {
        result = side_a.split(":")[0] +":"+ side_b.split(":")[1];
      }
      //logPrint("combine sides returning: " + result);
      return result;
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      output.collect(key, new Text(combineSides(
        new String(values.next().toString()), 
        new String(values.next().toString()))));
    }
  }

  public static class TriangleCountMap extends MapReduceBase implements Mapper<Text, Text, Text, Text>{
    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      StringPair vertices = new StringPair(key.toString(), ':');
      StringPair degrees = new StringPair(value.toString(), ':');
      
      int degree_of_first = Integer.parseInt(degrees.copyFirst());
      int degree_of_second = Integer.parseInt(degrees.copySecond());

      String min_vertex = degree_of_first < degree_of_second ? vertices.copyFirst() : vertices.copySecond();

      output.collect(new Text(min_vertex), key);
    }
  }
  private static class TriangleCountReduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int counter = 0;
      List<String> edges = new ArrayList<>();
      while(values.hasNext()){
        Text next_value = values.next();
        edges.add(new String(next_value.toString()));
        counter++;
      }
      
      String string_key = key.toString();
      for(int i = 0; i < counter; ++i){
        for(int j = i+1; j < counter; ++j){
          StringPair first_edge =  new StringPair(edges.get(i), ':');
          StringPair second_edge =  new StringPair(edges.get(j), ':');
          String vertex_a = first_edge.copyOtherPart(string_key);
          String vertex_b = second_edge.copyOtherPart(string_key);
          
          output.collect(new Text(vertex_a + ":" + vertex_b), new Text("Proposal"));
          output.collect(new Text(vertex_b + ":" + vertex_a), new Text("Proposal"));
        }
      }

    }
  }

  private static class TriangleCountReduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
      int counter = 0;
      List<String> values_list = new ArrayList<>();
      while(values.hasNext()){
        Text next_value = values.next();
        values_list.add(new String(next_value.toString()));
        counter++;
      }
      
      int num_proposals = 0;
      boolean edge_exists = false;
      for(int i = 0; i < counter; ++i){
        if(values_list.get(i).equals("Proposal")){
          num_proposals++;
        } else {
          edge_exists = true;
        }
      }
      
      if(edge_exists && num_proposals > 0){
        output.collect(new Text("Total:"), new Text(Integer.toString(num_proposals)));
      }
    }
  }

  private static class TriangleCountReduce3 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
      int counter = 0;
      List<String> values_list = new ArrayList<>();
      while(values.hasNext()){
        Text next_value = values.next();
        values_list.add(new String(next_value.toString()));
        counter++;
      }
      
      int total_triangles = 0;
      boolean edge_exists = false;
      for(int i = 0; i < counter; ++i){
        total_triangles += Integer.parseInt(values_list.get(i));
      }
      
      output.collect(new Text("Total:"), new Text(Integer.toString(total_triangles)));
    }
  }

  static private PrintStream console_log;
  static private boolean node_was_initialized = false;
  private static void logPrint(String line){
    if(!node_was_initialized){
      try{
        console_log = new PrintStream(new FileOutputStream("/tmp/hadoop_log.txt", true));
      } catch (FileNotFoundException e){
        return;
      }
      node_was_initialized = true;
    }
    console_log.println(line);
  }
  public static void logInput(String title, Text key, Text value){
    logPrint(title + ":\n\tkey:     " + key.toString() + "\n\tvalue:   " + value.toString());
  }

  private static JobConf createNewTextConf(){
    JobConf result = new JobConf(TriangleCountComputation.class);
    
    result.setOutputKeyClass(Text.class);
    result.setOutputValueClass(Text.class);
    result.setInputFormat(KeyValueTextInputFormat.class);
    result.setOutputFormat(TextOutputFormat.class);

    return result;
  }

  private static JobConf createNewTextConf(String input_path, String output_path){
    JobConf result = createNewTextConf();
  
    FileInputFormat.setInputPaths(result, new Path(input_path));
    FileOutputFormat.setOutputPath(result, new Path(output_path));

    return result;
  }

  private static void runCommand(String cmd) throws Exception{
    Runtime run = Runtime.getRuntime();
    Process pr = run.exec(cmd);
    pr.waitFor();
  }

  private static void runHadoopCommand(String cmd) throws Exception {
    runCommand("/usr/local/hadoop/bin/hadoop "+cmd);
  }

  private static void removeDirFromDFS(String file_path) throws Exception {
    runHadoopCommand("dfs -rmr "+file_path);
  }

  public static void main(String[] args) throws Exception {
    boolean count_degrees = true;
    boolean combine_totals = false;
  
    String degree_input_path = args[0];
    String degree_intermidiate_result_path = "/tmp/degree_intermediate";
    String degree_result_path = "/user/hduser/output/degree_output";
    String triangle_intermidiate_result_path1 = "/tmp/triangle_intermediate1";
    String triangle_intermidiate_result_path2 = "/tmp/triangle_intermediate2";
    String triangle_result_path = args[1];
  
    if(count_degrees){
      removeDirFromDFS(degree_intermidiate_result_path);
      removeDirFromDFS(degree_result_path);
    }
    removeDirFromDFS(triangle_intermidiate_result_path1);
    removeDirFromDFS(triangle_intermidiate_result_path2);
    removeDirFromDFS(triangle_result_path);

  // degree count part:
    if(count_degrees){
      JobConf degree_count_conf1 = createNewTextConf(degree_input_path, degree_intermidiate_result_path);
      degree_count_conf1.setMapperClass(DegreeCountMap.class);
      degree_count_conf1.setReducerClass(DegreeCountReduce1.class);
      JobClient.runJob(degree_count_conf1);

      JobConf degree_count_conf2 = createNewTextConf(degree_intermidiate_result_path, degree_result_path);
      degree_count_conf2.setReducerClass(DegreeCountReduce2.class);
      JobClient.runJob(degree_count_conf2);
    }
    
  // triangle count part:
    JobConf triangle_count_conf1 = createNewTextConf(degree_result_path, triangle_intermidiate_result_path1);
    triangle_count_conf1.setMapperClass(TriangleCountMap.class);
    triangle_count_conf1.setReducerClass(TriangleCountReduce1.class);
    JobClient.runJob(triangle_count_conf1);

    JobConf triangle_count_conf2 = createNewTextConf();
    FileInputFormat.setInputPaths(triangle_count_conf2, triangle_intermidiate_result_path1+","+degree_result_path);
    FileOutputFormat.setOutputPath(triangle_count_conf2, new Path(triangle_intermidiate_result_path2));
    triangle_count_conf2.setReducerClass(TriangleCountReduce2.class);
    JobClient.runJob(triangle_count_conf2);

    JobConf triangle_count_conf3 = createNewTextConf(triangle_intermidiate_result_path2, triangle_result_path);
    triangle_count_conf3.setReducerClass(TriangleCountReduce3.class);
    JobClient.runJob(triangle_count_conf3);

  }
}