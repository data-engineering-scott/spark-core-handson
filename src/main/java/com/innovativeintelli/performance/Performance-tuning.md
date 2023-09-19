Test changes with

spark.driver.cores
spark.driver.memory
executor-memory
executor-cores
spark.cores.max

reserve cores(1 core per node) for daemons
num_executors = total_cores/num_cores
num_partitions

Too much memory per executor can result in excessive GC delays
Too little memory can lose benefits from running multiple tasks in the single JVM
Look at stats(network, CPU, memory etc and tweak to improve performance)\

Use SparkUI, sysdig

Skew:
 Can severely downgrade performance
  1) Exterme imbalance of work in the cluster
  2) Tasks within a stage take uneven amounts of time to finish


How to check 

Spark UI job waiting only on some of the tasks
Look for large variances in memory usage within job(primarily works if it is at the begining of the job and doing data ingeestion - otherwise can ve misleading)
Executot missing heartbeat
check partition sizes of RDD while debugging to confirm

Check Spark System metrics such as:
CPU(cores), CPU percentage, Memory(bytes) and Memory(%)
Particularly check if any huge difference among those JVM , For exampe one of them takes 93% and other takes 10% of memory

SKEW in ingestion of data: Check if rdd is split evenly accross all the partitions. 
Solution of Handling skews: Blind repartition is the most native apprach but effective - Great for narrow transformations, good for increasing the number of partitions, use coalesce not repartion to decrease partitions

Cache/Persist

Resuse the dataframe with transformations
Unpersist when done
Without cache, Dataframe is buit from scratch each time
Don't over persist
    worse memory performance
    possible slowdown
    GC pressure


some options for easily improve the performance

Try seq.par.foreach instead of just seq.foreach
   Increase parallelization
   Race conditions and non-determinitistic results
   Use accumlators or synchronization to protect

Avoid udsf if possible
  Deserialize every row to object
  Apply lambda
  Then researialize it
  More garbage generated

Things to remember :

Keep Larger data frame left, spark by default try to shuffle the dataframe on the right
Follow good partitioning stratagies

Too few partitions - less parallelism
Too many partitions - scheduling issues
Improve scan efficiency
Try to use same partition between Dataframes for joins
Skipped stages are good
Caching prevent repeated exchnages where data is reused

Java Serialization can be slow sometimes explore kryoSerialization for the default data types

When working with Executors that is less than 32 GB, See CompressedOOPS JVM options which will allow use 4 bytes pointer instead of 8 bytes 
Example -XX:UseCompressedOOPS


Garbage collections:

Contention when allocating more objects
Full GC occuring before tasks complete
Too many RDD cached
Frequent garbage collection
Long time spent in gc
Missed heart beats
check the timeSpent on task vs garbage collection on spark UI

<img width="1028" alt="image" src="https://github.com/data-engineering-scott/spark-core-handson/assets/111550128/991572cb-4092-4896-b20f-972aa98e7a5f">


<img width="1019" alt="image" src="https://github.com/data-engineering-scott/spark-core-handson/assets/111550128/721c7866-6260-408e-bb64-ff7563ec742b">


![image](https://github.com/data-engineering-scott/spark-core-handson/assets/111550128/1a1aa554-56b8-4106-9d3f-a22545a99cc4)









