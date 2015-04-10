package:
	sbt assembly
test:
	spark-submit --class Wesqc --master local target/scala-2.10/wesqc-1.0.jar conf/mhgrid.test.conf
run:
	spark-submit --class Wesqc  --num-executors 65 target/scala-2.10/wesqc-1.0.jar conf/mhgrid.conf
testesp:
	spark-submit --class Wesqc --master local target/scala-2.10/wesqc-1.0.jar conf/esp.test.conf
runesp:
	spark-submit --class Wesqc  --num-executors 65 target/scala-2.10/wesqc-1.0.jar conf/esp.conf
install:
	cp target/scala-2.10/wesqc_2.10-1.0.jar ~/software/wesqc/
clean:
	rm -f *.txt *.csv
