package:
	sbt assembly
test:
	time spark-submit --class Wesqc --master local target/scala-2.10/wesqc-1.0.jar conf/mhgrid.test.conf
run:
	time spark-submit --class Wesqc  --num-executors 140 target/scala-2.10/wesqc-1.0.jar conf/mhgrid.conf
testesp:
	spark-submit --class Wesqc --master local target/scala-2.10/wesqc-1.0.jar conf/esp.test.conf
runespwhr:
	spark-submit --class Wesqc  --num-executors 270 target/scala-2.10/wesqc-1.0.jar conf/esp.whr.conf 0-2
runespheight:
	spark-submit --class Wesqc  --num-executors 270 target/scala-2.10/wesqc-1.0.jar conf/esp.height.conf
install:
	cp target/scala-2.10/wesqc_2.10-1.0.jar ~/software/wesqc/
clean:
	rm -rf /mnt/ceph/user/zhangdi/esp.whr
