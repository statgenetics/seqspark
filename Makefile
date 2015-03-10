package:
	sbt package
test:
	spark-submit --class Test --master local target/scala-2.10/test_2.10-1.0.jar test.vcf.bz2
run:
	spark-submit --class Test  --num-executors 39 target/scala-2.10/test_2.10-1.0.jar mhgrid_annotated.vcf.bz2
clean:
	rm -f *.txt *.csv
