package com.hipi;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HelloWorld extends Configured implements Tool {

  public int run(String[] args) throws Exception {

	 System.out.println("Hello HIPI!");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HelloWorld(), args);
    System.exit(res);
  }

}

