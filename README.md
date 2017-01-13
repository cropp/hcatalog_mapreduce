# hcatalog_mapreduce

Thank you for taking a look at my example project. I hope it saves you many hours of research, head banging, and hair pulling.

### A couple good things about this project

Works with your own hadoop

Works with aws mapreduce / EMR

Works to read files from S3 and write files to S3 <-- BIG WIN -->

This works by creating both the input table and output tables as external. Then using HCatalog as both input and ouput obfuscating where it is getting data from or writing data to.

### Things to note
Because of the added requirements on hcatalog and hive make sure to add these to your hadoop classpath before execution.

How I did it.
 
\#>hadoop classpath

This outputs what the current classpath hadoop has. Edit the begining where it lists something like /etc/hadoop/conf:blah lib:blah lib to

/etc/hadoop/conf:/etc/hive-hcatalog/conf:/etc/hive/conf:blah lib: blah lib

\#>export HADOOP_CLASSPATH=^^^^
\#>hadoop jar tlmclean-job.jar "dt => '2017-01-01'"



