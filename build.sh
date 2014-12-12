MOBILIUM_HOME=/opt/mobileum
mvn clean install -DskipTests=true -Dair.check.skip-license=true -Dcheckstyle.skip=true

cp -R presto-server/target/presto-server-0.82 $MOBILIUM_HOME/
cp -R presto-server/target/etc $MOBILIUM_HOME/presto-server-0.82/
cp presto-cli/target/presto-cli-0.82-executable.jar $MOBILIUM_HOME/presto-server-0.82/presto
chmod +x $MOBILIUM_HOME/presto-server-0.82/presto

cd $MOBILIUM_HOME
rm build-*.tar.gz

timestamp=`date +'%Y%m%d%H%M'`
if [ "-Pprod" != "$1" ]
then
  tar zcvf build-$timestamp.tar.gz *
  else
    tar zcvf build-hdfs2-$timestamp.tar.gz *
fi
