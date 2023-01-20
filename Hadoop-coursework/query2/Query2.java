import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang3.builder.CompareToBuilder;

public class Query2 {

  /**
   * Overrides compare method of Comparator class
   */
  public static class RecordComparator implements Comparator<Record> {
    @Override
    /**
     * Sort two records in descending order of floor space then in descending order of net_paid
     */
    public int compare(Record r1, Record r2) {
      return new CompareToBuilder().append(r2.getFloor(), r1.getFloor())
                                   .append(r2.getNetPaid(), r1.getNetPaid()).toComparison();
    }
  }

  /**
   * The record class
   */
  public static class Record {
    private String storeNumber;
    private Long floor;
    private Double netPaid;

    // class constructor
    public Record(String storeNumber, Double netPaid, Long floor) {
      this.storeNumber = storeNumber;
      this.netPaid = netPaid;
      this.floor = floor;
    }

    /**
     * 
     * @return store number
     */
    public String getStoreNumber() {
      return storeNumber;
    }

    /**
     * 
     * @return floor space
     */
    public Long getFloor() {
      return floor;
    }

    /**
     * 
     * @return net paid
     */
    public Double getNetPaid() {
      return netPaid;
    }
  }

  /**
   * Mapper for the store_sales table
   */
  public static class SalesMapper
       extends Mapper<Object, Text, Text, Text>{

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
          String netPaid = parts[20].toString();

          // map (k, v) to (k', v') where both k' and v' are both text
          // Example of a (k', v') tuple: ("2", "Sales  45.67")
          context.write(new Text(store), new Text("Sales\t" + netPaid));
        }
      }
    }
  }

  /**
   * Mapper for the store table
   */
  public static class StoreMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String record = value.toString();
      String[] parts = record.split("\\|");
      // check if record has at least 8 fields with NO NULL values and check that required fields have no NULL values
      if (parts.length >= 8 && null != record && !parts[0].equals("") && !parts[7].equals("")) {
        String store = parts[0].toString();
        String floor = parts[7].toString();

        // map (k, v) to (k', v') where both k' and v' are both text
        // Example of a (k', v') tuple: ("2", "Store  95513")
        context.write(new Text(store), new Text("Store\t" + floor));
      }
    }
  }

  public static class ReduceJoinReducer
       extends Reducer<Text,Text,Text,Text> {
    
    private int n; // top n records needed
    private List<Record> records; // An array list of type 'Record'

    @Override
    /**
     * Initialise n and array list
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n = Integer.parseInt(context.getConfiguration().get("k"));
      records = new ArrayList<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      long floorSpace = 0l; 
      double total = 0.0; // net paid
      for (Text t : values) {
        String parts[] = t.toString().split("\t"); // an array of strings of length 2
        if (parts[1].equals("")) {
          continue;
        }
        if (parts[0].equals("Sales")) { // tuple obtained from store_sales table
          total += Double.parseDouble(parts[1]);
        }
        else if (parts[0].equals("Store")) { // tuple obtained from store table
          floorSpace =Long.parseLong(parts[1]);
        }
      }
      records.add(new Record(key.toString(), total, floorSpace));
    }

    @Override
    /**
     * Called once at the end of the reduce job
     * @param context the configuration data and interfaces for emitting output
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      // do not sort in this stage
      // sorting is handled by the second job
      for (Record record : records) {
        // output of first job in the format:
        //"store_number"  "net_paid  floor_space" e.g. "2"  "45.67  95113"
        context.write(new Text(String.valueOf(record.getStoreNumber())), new Text(String.valueOf(record.getNetPaid() + "\t" + String.valueOf(record.getFloor()))));
      }
    }
  }

  /**
   * Second mapper required for sorting the data
   */
  public static class TopNMapper extends Mapper<Object, Text, Text, Text> {
    private int n; // top n records
    private List<Record> records; // list of records

    @Override
    /**
     * initialise n and the array list
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n =Integer.parseInt(context.getConfiguration().get("k"));
      records = new ArrayList<>();
    }

    public void map(Object key, Text value, Context context) {
      String[] parts = value.toString().split("\t"); // splits values in value and converts them to string
      records.add(new Record(parts[0].toString(), Double.parseDouble(parts[1]), Long.parseLong(parts[2])));
    }

    /**
     * Called once at the end of the map job
     * @param context the configuration data and interfaces for emitting output
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (Record record : records) {
        if (record.getNetPaid() <= 0.0d) { // if net paid is 0.0 skip to next record
          continue;
        }
        // map (k, v) to (k', v') where both k' and v' are both text
        // Example of a (k', v') tuple: ("2", "45.67  95513") i.e. ("store_number", "net_paid   floor_space")
        context.write(new Text(String.valueOf(record.getStoreNumber())), new Text(String.valueOf(record.getNetPaid() + "\t" + String.valueOf(record.getFloor()))));
      }
    }
  }

  /**
   * Second reducer: this is where sorting of data happens
   */
  public static class TopNReducer extends Reducer<Text, Text, Text, Text> {
    private int n; // top n redords
    private List<Record> topRecords; // an array list for storing top records

    @Override
    /**
     * Initialise n and the array list
     * @param context the configuration data and interfaces for emitting output
     */
    public void setup(Context context) {
      n = Integer.parseInt(context.getConfiguration().get("k"));
      topRecords = new ArrayList<>();
    }

    /**
     * add the (k', v') pairs to topRecords array list
     */
    public void reduce(Text key, Iterable<Text> values, Context context) {

      for (Text value : values) { // for loop executed only once as there is only one tuple of values
        String[] parts = value.toString().split("\t"); // splits value into individual parts. Array has length 2
        topRecords.add(new Record(key.toString(), Double.parseDouble(parts[0]), Long.parseLong(parts[1])));
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
      Collections.sort(topRecords, new RecordComparator());

      for (int i = 0; i < n; i ++) {
        if (i < topRecords.size()) { // make sure i does not exceed array size otherwise ArrayIndexOutOfBounds erorr
          Record record = topRecords.get(i);
          // Example: ("2", "45.67  95513") i.e. ("store_number", "net_paid   floor_space")
          context.write(new Text(String.valueOf(record.getStoreNumber())), new Text(String.valueOf(record.getNetPaid() + "\t" + String.valueOf(record.getFloor()))));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length != 6) { // if wrong number of arguments, output error
      System.err.println("Error: Incorrect number of arguments. Expected 6, but got " + args.length + ".");
      System.exit(2);
    }

    Path outputPath = new Path(args[5]);
  
    conf.set("k", args[0]);
    conf.set("start_date", args[1]);
    conf.set("end_date", args[2]);

    Job job = new Job(conf, "query 2");
    job.setJarByClass(Query2.class);
    // job.setCombinerClass(ReduceJoinReducer.class);
    job.setReducerClass(ReduceJoinReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, SalesMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[4]), TextInputFormat.class, StoreMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(outputPath, "out5"));

    if (!job.waitForCompletion(true)) {
      System.exit(1);
    }

    Job sort = Job.getInstance(conf, "sort");
    sort.setJarByClass(Query2.class);
    sort.setMapperClass(TopNMapper.class);
    sort.setReducerClass(TopNReducer.class);
    sort.setMapOutputKeyClass(Text.class);
    sort.setMapOutputValueClass(Text.class);
    sort.setOutputKeyClass(Text.class);
    sort.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(sort, new Path(outputPath, "out5"));
    FileOutputFormat.setOutputPath(sort, new Path(outputPath, "out6"));

    if (!sort.waitForCompletion(true)) {
      System.exit(1);
    }
  }
}
