
package tr.name.sualp.merter.hadoop.myjam;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class LikedJamCountMapperReducerTest {
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  @Before
  public void setUp() {
    LikedJamCount.TokenizerMapper mapper = new LikedJamCount.TokenizerMapper();
    LikedJamCount.IntSumReducer reducer = new LikedJamCount.IntSumReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() {
    mapDriver.withInput(new LongWritable(), new Text(
          "c1066039fa61eede113878259c1222d1\t5d2bc46196d7903a5580f0dbedc09610"));
    mapDriver.withOutput(new Text("5d2bc46196d7903a5580f0dbedc09610"), new IntWritable(1));
    try {
      mapDriver.runTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testReducer() {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("5d2bc46196d7903a5580f0dbedc09610"), values);
    reduceDriver.withOutput(new Text("5d2bc46196d7903a5580f0dbedc09610"), new IntWritable(2));
    try {
      reduceDriver.runTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMapReduce() {
    List<Pair<LongWritable, Text>> inputLines = new ArrayList<Pair<LongWritable, Text>>(3);
    Pair<LongWritable, Text> i1 = new Pair<LongWritable, Text>(new LongWritable(1), new Text(
          "c1066039fa61eede113878259c1222d1\t5d2bc46196d7903a5580f0dbedc09610"));
    Pair<LongWritable, Text> i2 = new Pair<LongWritable, Text>(new LongWritable(2), new Text(
          "c1066039fa61eede113878259c1222dl\t5d2bc46196d7903a5580f0dbedc09610"));
    Pair<LongWritable, Text> i3 = new Pair<LongWritable, Text>(new LongWritable(3), new Text(
          "c1066039fa61eede113878259c1222d1\t5d2bc46l96d7903a5580f0dbedc09610"));
    inputLines.add(i1);
    inputLines.add(i2);
    inputLines.add(i3);
    mapReduceDriver.withAll(inputLines);
    List<Pair<Text, IntWritable>> outputLines = new ArrayList<Pair<Text, IntWritable>>(2);
    Pair<Text, IntWritable> o1 = new Pair<Text, IntWritable>(new Text("5d2bc46196d7903a5580f0dbedc09610"), new IntWritable(2));
    Pair<Text, IntWritable> o2 = new Pair<Text, IntWritable>(new Text("5d2bc46l96d7903a5580f0dbedc09610"), new IntWritable(1));
    outputLines.add(o1);
    outputLines.add(o2);
    mapReduceDriver.withAllOutput(outputLines);
    try {
      mapReduceDriver.runTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
