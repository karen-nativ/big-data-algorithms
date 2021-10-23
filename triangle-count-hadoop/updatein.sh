source conf.sh

$HD dfs -rm /user/hduser/input/$INFILE
$HD dfs -put input/$INFILE /user/hduser/input/$INFILE
