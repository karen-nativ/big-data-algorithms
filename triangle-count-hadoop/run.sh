source conf.sh

IN_PATH=/user/hduser/input/$INFILE
OUT_PATH=/user/hduser/output/triangle_output

rm /tmp/hadoop_log.txt
$HD jar /home/hduser/mapred/$TARGET.jar org.computations.$TARGET $IN_PATH $OUT_PATH
if [ $? -eq 0 ]; then
	$HD dfs -cat $OUT_PATH/part-*
fi
cat /tmp/hadoop_log.txt
