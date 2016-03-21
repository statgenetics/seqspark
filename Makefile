package:
	sbt assembly
upload:
	scp target/scala-2.10/SeqSpark-1.0.jar csg:~/software/seqa/ 
	scp -r src/scripts/ csg:~/software/seqa/
test:
	time spark-submit --class SeqA --master local target/scala-2.10/SeqA-1.0.jar conf/mhgrid.test.conf
runmhgrid:
	time spark-submit --class SeqA  --num-executors 149 target/scala-2.10/SeqA-1.0.jar conf/mhgrid.conf
runespwhr:
	spark-submit --class SeqA  --num-executors 149 target/scala-2.10/SeqA-1.0.jar conf/esp.whr.conf 4
runespheight:
	spark-submit --class SeqA  --num-executors 149 target/scala-2.10/SeqA-1.0.jar conf/esp.height.conf 4
