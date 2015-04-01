package:
	sbt package
test:
	spark-submit --class Test --master local target/scala-2.10/test_2.10-1.0.jar test.esp.bz2
run:
	spark-submit --class Test  --num-executors 65 target/scala-2.10/test_2.10-1.0.jar esp.bz2
run2:
	spark-submit --class Test  --num-executors 65 target/scala-2.10/test_2.10-1.0.jar file:///mnt/nfs/zhangdi/project/mhgrid/mhgrid_raw.vcf
install:
	cp target/scala-2.10/wesqc_2.10-1.0.jar 
clean:
	rm -f *.txt *.csv
