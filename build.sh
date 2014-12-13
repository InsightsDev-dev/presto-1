MOBILIUM_HOME=/opt/mobileum
if [ $1 != "-Pdev" ]  && [ $1 != "-Pprod" ]
then
        echo "incorrect profile"
        exit 10;
fi

mvn clean install -DskipTests=true -Dair.check.skip-license=true -Dcheckstyle.skip=true



##temporary fix for copying jars according to profile
if [ $1 == "-Pdev" ]
then
cp ~/.m2/repository/net/java/dev/jets3t/jets3t/0.7.1/jets3t-0.7.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/commons/commons-compress/1.4.1/commons-compress-1.4.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/net/sf/kosmosfs/kfs/0.3/kfs-0.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/commons/commons-math/2.1/commons-math-2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/antlr/ST4/4.0.4/ST4-4.0.4.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/mobileum/common/3.0-SNAPSHOT/common-3.0-SNAPSHOT.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/antlr/antlr-runtime/3.4/antlr-runtime-3.4.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-digester/commons-digester/1.8/commons-digester-1.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-xc/1.7.1/jackson-xc-1.7.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.8.8/jackson-mapper-asl-1.8.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/tomcat/jasper-runtime/5.5.12/jasper-runtime-5.5.12.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/antlr/stringtemplate/3.2.1/stringtemplate-3.2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/tukaani/xz/1.0/xz-1.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/avro/avro/1.7.4/avro-1.7.4.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/sun/jersey/jersey-json/1.8/jersey-json-1.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/thoughtworks/paranamer/paranamer/2.3/paranamer-2.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-net/commons-net/1.4.1/commons-net-1.4.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/pig/pig/0.12.0/pig-0.12.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/mortbay/jetty/jsp-api-2.1/6.1.14/jsp-api-2.1-6.1.14.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.8.8/jackson-core-asl-1.8.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/xerial/snappy/snappy-java/1.1.1.3/snappy-java-1.1.1.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/hadoop/hadoop-core/1.2.1/hadoop-core-1.2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-jaxrs/1.7.1/jackson-jaxrs-1.7.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/mortbay/jetty/jetty/6.1.26/jetty-6.1.26.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/javax/xml/stream/stax-api/1.0-2/stax-api-1.0-2.jar presto-server/target/presto-server-0.82/plugin/metadata/
elif [ $1 == "-Pprod" ] 
then
cp ~/.m2/repository/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/commons/commons-compress/1.4.1/commons-compress-1.4.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/net/sf/kosmosfs/kfs/0.3/kfs-0.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/commons/commons-math/2.1/commons-math-2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/antlr/ST4/4.0.4/ST4-4.0.4.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/mobileum/common/3.0-SNAPSHOT/common-3.0-SNAPSHOT-hdfs2.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/antlr/antlr-runtime/3.4/antlr-runtime-3.4.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-digester/commons-digester/1.8/commons-digester-1.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.8.8/jackson-mapper-asl-1.8.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-net/commons-net/3.1/commons-net-3.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/hadoop/hadoop-auth/2.2.0/hadoop-auth-2.2.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/antlr/stringtemplate/3.2.1/stringtemplate-3.2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-daemon/commons-daemon/1.0.13/commons-daemon-1.0.13.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/sun/jersey/jersey-json/1.9/jersey-json-1.9.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/tukaani/xz/1.0/xz-1.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/avro/avro/1.7.4/avro-1.7.4.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/net/java/dev/jets3t/jets3t/0.6.1/jets3t-0.6.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/thoughtworks/paranamer/paranamer/2.3/paranamer-2.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/hadoop/hadoop-annotations/2.2.0/hadoop-annotations-2.2.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/pig/pig/0.12.0/pig-0.12.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-xc/1.8.3/jackson-xc-1.8.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.8.8/jackson-core-asl-1.8.8.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/com/jcraft/jsch/0.1.42/jsch-0.1.42.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/xerial/snappy/snappy-java/1.1.1.3/snappy-java-1.1.1.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/hadoop/hadoop-hdfs/2.2.0/hadoop-hdfs-2.2.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/javax/servlet/jsp/jsp-api/2.1/jsp-api-2.1.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/apache/hadoop/hadoop-common/2.2.0/hadoop-common-2.2.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/codehaus/jackson/jackson-jaxrs/1.8.3/jackson-jaxrs-1.8.3.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar presto-server/target/presto-server-0.82/plugin/metadata/
cp ~/.m2/repository/org/mortbay/jetty/jetty/6.1.26/jetty-6.1.26.jar presto-server/target/presto-server-0.82/plugin/metadata/
else 
        echo "the profile : $1 is not configured"
        exit 10
fi

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
