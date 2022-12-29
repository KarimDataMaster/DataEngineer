export MR_OUTPUT=/user/root/output-data

hadoop fs -rm -r $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Simple treaming job reduce' \
-Dmapred.reduce.tasks=1 \
-file tmp/mapreduce/mapper.py -mapper tmp/mapreduce/mapper.py \
-file tmp/mapreduce/reducer.py -reducer tmp/mapreduce/reducer.py \
-input /user/root/input-data -output $MR_OUTPUT
#-input 2020/2020/ -output $MR_OUTPUT


#-input s3://step5/ny-taxi/ -output $MR_OUTPUT
#-input aws --profile=k-valiev --endpoint-url=https://storage.yandexcloud.net s3 --recursive s3://step5/ -output $MR_OUTPUT
#-input s3://step5/ny-taxi/ -output $MR_OUTPUT