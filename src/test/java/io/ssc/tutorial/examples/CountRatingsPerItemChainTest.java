package io.ssc.tutorial.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class CountRatingsPerItemChainTest {

  @Test
  public void testMapper() throws Exception {
    Mapper.Context ctx = Mockito.mock(Mapper.Context.class);

    CountRatingsPerItemChain.SingleRatingMapper mapper = new CountRatingsPerItemChain.SingleRatingMapper();

    mapper.map(null, new Text("123,456,3.5"), ctx);

    Mockito.verify(ctx).write(new LongWritable(456), new IntWritable(1));
  }

  @Test
  public void testReducer() throws Exception {
    Reducer.Context ctx = Mockito.mock(Reducer.Context.class);

    CountRatingsPerItemChain.SumRatingsReducer reducer = new CountRatingsPerItemChain.SumRatingsReducer();

    reducer.reduce(new LongWritable(1), Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(2)), ctx);

    Mockito.verify(ctx).write(new LongWritable(1), new IntWritable(4));
  }
}
