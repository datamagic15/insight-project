# insight-project
kafka-streaming is the producer that sends data to kafka.
It takes 2 arguments

1) dev (local run) or prod( aws run )
2) The file to stream

SparkStreamingJob is the job that consumes the data and 
performs aggregations.
It takes 1 argument

1) dev (local run) or prod( aws run )

