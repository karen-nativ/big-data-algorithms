source conf.sh
sudo rm -r $TARGET
sudo rm $TARGET.jar

sudo javac -classpath $HD_CORE_JAR -d $TARGET src/*.java
tree $TARGET
sudo jar -cvf /home/hduser/mapred/$TARGET.jar -C $TARGET/ .
