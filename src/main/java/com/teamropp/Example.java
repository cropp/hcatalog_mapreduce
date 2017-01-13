package com.teamropp;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.MultiOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by cropp<cropp@teamropp.com> on 1/12/17.
 * <p>
 * Notes:
 * This particular example has not been executed on EMR so I would like any feedback
 * I believe you need EMRFS enabled
 */
public class Example extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(Example.class);
    private static final String OUTKEY = "OUTTABLE";

    public Example(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {

        String inputTablePartitionFilter = args[0];
        String inputTableSchema = "default";
        String inputTableName = "loading";

        /*
        * --input table for this example
        * create external table if not exists default.loading (string raw)
        * partitioned by (dt date)
        * location 's3://bucket/';
        *
        * -- The following command will attempt to add all the partitions
        * msck repair table default.loading;
        * */

        String outputTableSchema = "default";
        String outputTableName = "loaded";

        /*
        * -- ouput table for this example
        * create external table if not exists default.loaded (string raw)
        * partitioned by (dt date)
        * location 's3://bucket/output';
        *
        * Running the job will create the s3 partitions but we still need to
        * define that the output table has dynamic partitions. See below
        * */


        Job job = Job.getInstance(getConf(), "HCatalogMapreduceExample");
        job.setJarByClass(Example.class);
        job.setInputFormatClass(HCatInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Allows writing to multiple tables from single input
        job.setOutputFormatClass(MultiOutputFormat.class);
        job.setMapperClass(Map.class);

        // This is a straight map without reduce
        job.setNumReduceTasks(0);

        // Setting Job Input
        // inputTablePartitionFilter for this example would be "dt = '2017-01-01'" or "dt >= '2017-01-01'"
        // If left null would consume all partitions
        HCatInputFormat.setInput(job, inputTableSchema, inputTableName, inputTablePartitionFilter);


        // Setting Job Output
        MultiOutputFormat.JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);

        List<String> dynamicPartitions = Lists.newArrayList("dt");
        // If you new the partitions you could define them here, null for dynamic
        OutputJobInfo simpleJobInfo = OutputJobInfo.create(outputTableSchema, outputTableName, null);
        simpleJobInfo.setDynamicPartitioningKeys(dynamicPartitions);

        configurer.addOutputFormat(OUTKEY, HCatOutputFormat.class, NullWritable.class, HCatRecord.class);
        HCatOutputFormat.setOutput(configurer.getJob(OUTKEY), simpleJobInfo);
        // Telling the schema what type of dynamic field our partition is -- in this case date type info
        HCatFieldSchema txnDate = new HCatFieldSchema("dt", TypeInfoFactory.dateTypeInfo, null);
        HCatSchema simpleSchema = HCatOutputFormat.getTableSchema(configurer.getJob(OUTKEY).getConfiguration());
        simpleSchema.append(txnDate);
        HCatOutputFormat.setSchema(configurer.getJob(OUTKEY), simpleSchema);

        // OutputJobInfo invalidJobInfo = OutputJobInfo.create(otherOutputSchema, otherOuputName, null);
        // configurer.addOutputFormat(INVALID_KEY, HCatOutputFormat.class, NullWritable.class, HCatRecord.class);
        // HCatOutputFormat.setOutput(configurer.getJob(INVALID_KEY), invalidJobInfo);
        // HCatOutputFormat.setSchema(configurer.getJob(INVALID_KEY),
        // HCatOutputFormat.getTableSchema(configurer.getJob(INVALID_KEY).getConfiguration()));

        configurer.configure();
        int retval = (job.waitForCompletion(true) ? 0 : 1);

        // Do other work if you want here
        return retval;
    }

    public static class Map extends Mapper<WritableComparable, HCatRecord, HCatRecord, NullWritable> {

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            logger.info("Starting out Example HCatalogMapreduce Mapper");
        }

        @Override
        protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
            // 0 represents the column 'raw'
            String line = value.get(0).toString();
            // Make sure that the last values are that of your partition keys
            // You can get it from the key, or extract it from line or make up random dates based on
            // temp humidity and the speed of the cars going by your window
            MultiOutputFormat.write(OUTKEY
                    , createHCatRecord(line, "2016-01-01")
                    , NullWritable.get(),
                    context);

        }
    }

    private static HCatRecord createHCatRecord(Object... fields) {
        HCatRecord record = new DefaultHCatRecord(fields.length);
        for (int x = 0; x < fields.length; x++) {
            record.set(x, fields[x]);
        }
        return record;
    }

    public static void main(String[] args) throws Exception {
        new Example(new Configuration()).run(args);
    }
}
