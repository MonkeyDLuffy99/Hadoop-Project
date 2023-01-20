import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QueryA {

  /**
   * Mapper for the store_sales table
   */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      long start_date = Long.parseLong(conf.get("start_date"));
      long end_date = Long.parseLong(conf.get("end_date"));
      String record = value.toString();
      String[] parts = record.split("\\|");
      // check if record has at least 21 fields with NO NULL values and check that required fields have no NULL values
      if (parts.length >= 21 && null != record && !parts[0].equals("") && !parts[7].equals("") && !parts[20].equals("")) {
        long current_date = Long.parseLong(parts[0]);
        if (start_date <= current_date && current_date <= end_date) { // if date within desired range
          String store = parts[7].toString();
          Double netPaid = Double.parseDouble(parts[20]);

          // map (k, v) to (k', v') where k' is Text and v' is DoubleWritable
          // Example of a (k', v') tuple: ("2", 45.67)
          context.write(new Text(store), new DoubleWritable(netPaid));
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    
    private DoubleWritable result = new DoubleWritable(); // the result after iterating over the value tuples

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      Double total_value = 0.0d;
      for (DoubleWritable val : values) { // iterate over all values
          total_value += val.get();
      }
      
      result.set(total_value);

      // Example ("2", 8918564.1342)
      context.write(key, result);
    }
  }

  /**
   * Second mapper required for sorting the data
   */
  public static class TopNMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private int n; // top n records needed
    private TreeMap<Double, String> tmap; // A tree map with key as Double and value as String

    @Override
    /**
     * Initialise n and tree map
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n = Integer.parseInt(context.getConfiguration().get("k"));
      tmap = new TreeMap<Double, String>();
    }

    public void map(Object key, Text value, Context context) {
      String[] parts = value.toString().split("\t"); // splits values in value and converts them to string

      // example of an element of the map: key: 45.67, value: "2"
      tmap.put(Double.valueOf(parts[1]), parts[0]); // add net_paid to key and store_number to value

      if (tmap.size() > n) { // since values of tree map are already sorted, if map size exceeds value of top N records remove element with smallest key value
        tmap.remove(tmap.firstKey()); // first key has the smallest value
      }
    }

    /**
     * Called once at the end of the map job
     * @param context the configuration data and interfaces for emitting output
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Double, String> entry : tmap.entrySet()) { // iterate through map

        // map (k, v) to (k', v') where k' is Text v' is DoubleWritable
        // Example of a (k', v') tuple: ("2", 45.67) i.e. ("store_number", net_paid)
        context.write(new Text(entry.getValue()), new DoubleWritable(entry.getKey()));
      }
    }
  }

  /**
  * Second reducer: this is where sorting of data happens
  */
  public static class TopNReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private int n; // top n redords
    private TreeMap<Double, String> tmap2; // A tree map with key as Double and value as String

    @Override
    /**
     * Initialise n and tree map
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n = Integer.parseInt(context.getConfiguration().get("k"));
      tmap2 = new TreeMap<Double, String>();
    }

    /**
     * add the (k', v') pairs to tree map
     */
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
      double total = 0.0d;

      for (DoubleWritable value : values) { // for loop executed only once as there is only one tuple of values
        total = value.get();
      }

      tmap2.put(total, key.toString()); // add net_paid to key and store_number to value

      if (tmap2.size() > n) { // since values of tree map are already sorted, if map size exceeds value of top N records remove element with smallest key value
        tmap2.remove(tmap2.firstKey());  // first key has the smallest value
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
      Set<Double> keySet = tmap2.descendingKeySet(); // use a hash set for storing keys in descending order
      for (Double key : keySet) { // iterate through hash set
        // get the value from tree map with the required key, get value of the hash set (hash set stores all keys from tree map in descending order)
        // example output: ("2", 45.67)
        context.write(new Text(tmap2.get(key)), new DoubleWritable(key));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 5) { // if wrong number of arguments, output error
      System.err.println("Error: Incorrect number of arguments. Expected 5, but got " + args.length + ".");
      System.exit(2);
    }
    
    Path out = new Path(args[4]);
  
    conf.set("k", args[0]);
    conf.set("start_date", args[1]);
    conf.set("end_date", args[2]);

    Job job = Job.getInstance(conf, "query a");
    job.setJarByClass(QueryA.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(out, "out5"));

    if (!job.waitForCompletion(true)) {
      System.exit(1);
    }

    Job sort = Job.getInstance(conf, "sort");
    sort.setJarByClass(QueryA.class);
    sort.setMapperClass(TopNMapper.class);
    sort.setReducerClass(TopNReducer.class);
    sort.setMapOutputKeyClass(Text.class);
    sort.setMapOutputValueClass(DoubleWritable.class);
    sort.setOutputKeyClass(Text.class);
    sort.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(sort, new Path(out, "out5"));
    FileOutputFormat.setOutputPath(sort, new Path(out, "out6"));

    if (!sort.waitForCompletion(true)) {
      System.exit(1);
    }
  }
}
