import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QueryB {

   /**
   * Mapper for the store_sales table
   */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      long start_date = Long.parseLong(conf.get("start_date"));
      long end_date = Long.parseLong(conf.get("end_date"));
      String record = value.toString();
      String[] parts = record.split("\\|");
      // check if record has at least 10 fields with NO NULL values and check that required fields have no NULL values
      if (parts.length > 10 && !parts[0].equals("") && !parts[2].equals("") && !parts[10].equals("")) {
        long current_date = Long.parseLong(parts[0]);
        if (start_date <= current_date && current_date <= end_date) { // if date within desired range
          String item = parts[2].toString();
          int quantity = Integer.parseInt(parts[10]);

          // map (k, v) to (k', v') where k' is Text and v' is IntWritable
          // Example of a (k', v') tuple: ("11", 64)
          context.write(new Text(item), new IntWritable(quantity));
        }
      }	 
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable(); // the result after iterating over the value tuples

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int total_value = 0;
      for (IntWritable val : values) { // iterate over all values
          total_value += val.get();
      }
      result.set(total_value);

      // Example ("11", 768)
      context.write(key, result);
    }
  }

  /**
   * Second mapper required for sorting the data
   */
  public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {
    private int n; // top n records needed
    private TreeMap<Integer, String> tmap; // A tree map with key as Integer and value as String

    @Override
    /**
     * Initialise n and tree map
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n = Integer.parseInt(context.getConfiguration().get("k"));
      tmap = new TreeMap<Integer, String>();
    }

    public void map(Object key, Text value, Context context) {
      String[] parts = value.toString().split("\t"); // splits values in value and converts them to string

      // example of an element of the map: key: 768, value: "11"
      tmap.put(Integer.valueOf(parts[1]), parts[0]); // add item to key and quantity to value


      if (tmap.size() > n) { // since values of tree map are already sorted, if map size exceeds value of top N records remove element with smallest key value
        tmap.remove(tmap.firstKey());  // first key has the smallest value
      }
    }

    /**
     * Called once at the end of the map job
     * @param context the configuration data and interfaces for emitting output
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Integer, String> entry : tmap.entrySet()) { // iterate through map

        // map (k, v) to (k', v') where k' is Text v' is IntWritable
        // Example of a (k', v') tuple: ("11", 768) i.e. ("item", quantity)
        context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
      }
    }
  }

  /**
  * Second reducer: this is where sorting of data happens
  */
  public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int n; // top n redords
    private TreeMap<Integer, String> tmap2; // A tree map with key as Integer and value as String

    @Override
    /**
     * Initialise n and tree map
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n = Integer.parseInt(context.getConfiguration().get("k"));
      tmap2 = new TreeMap<Integer, String>();
    }

    /**
     * add the (k', v') pairs to tree map
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
      int total = 0;

      for (IntWritable value : values) { // for loop executed only once as there is only one tuple of values
        total = value.get();
      }

      tmap2.put(total, key.toString()); // add quantity to key and item to value
 
      if (tmap2.size() > n) { // since values of tree map are already sorted, if map size exceeds value of top N records remove element with samllest key value
        tmap2.remove(tmap2.firstKey()); // first key has the smallest value
      }
    }

    @Override
    /**
     * Called once at the end of the reduce job
     * @param context the configuration data and interfaces for emitting output
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      Set<Integer> keySet = tmap2.descendingKeySet(); // use a hash set for storing keys in descending order
      for (Integer key : keySet) {  // iterate through hash set

        // get the value from tree map with the required key, get value of the hash set (hash set stores all keys from tree map in descending order)
        // example output: ("11", 768)
        context.write(new Text(tmap2.get(key)), new IntWritable(key)); 
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 5) {  // if wrong number of arguments, output error
      System.err.println("Error: Incorrect number of arguments. Expected 5, but got " + args.length + ".");
      System.exit(2);
    }

    Path out = new Path(args[4]);

    conf.set("k", args[0]);
    conf.set("start_date", args[1]);
    conf.set("end_date", args[2]);

    Job job = Job.getInstance(conf, "query b");
    job.setJarByClass(QueryB.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(out, "out5"));

    if (!job.waitForCompletion(true)) {
      System.exit(1);
    }

    Job sort = Job.getInstance(conf, "sort");
    sort.setJarByClass(QueryB.class);
    sort.setMapperClass(TopNMapper.class);
    sort.setReducerClass(TopNReducer.class);
    sort.setMapOutputKeyClass(Text.class);
    sort.setMapOutputValueClass(IntWritable.class);
    sort.setOutputKeyClass(Text.class);
    sort.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(sort, new Path(out, "out5"));
    FileOutputFormat.setOutputPath(sort, new Path(out, "out6"));

    if (!sort.waitForCompletion(true)) {
      System.exit(1);
    }
  }
}
