
package com.example.bigtable.sample.test;

import com.example.bigtable.sample.DictionaryDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class DictionaryTest {

  public static void main(String[] args) throws IOException {
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> driver =
            new MapReduceDriver<>();

    driver.withMapper(new DictionaryDriver.TokenizerMapper())
            .withReducer(new DictionaryDriver.IntSumReducer())
            .withInput(new Text("1st input") , new Text("aaa"))
            .withInput(new Text("2nd input"), new Text("bbb"))
            .withOutput(new Text("a"), new IntWritable(3))
            .withOutput(new Text("b"), new IntWritable(3))
            .runTest();

  }

}
