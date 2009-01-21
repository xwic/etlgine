@ECHO OFF
REM COMMONS
SET ETLCP=lib/commons-collections-2.1.jar;lib/commons-dbcp-1.2.1.jar;lib/commons-lang-2.1.jar;lib/commons-logging.jar;lib/commons-pool-1.2.jar
SET ETLCP=%ETLCP%;lib/dom4j-1.5.2.jar;lib/jaxen-1.1.1.jar
SET ETLCP=%ETLCP%;lib/groovy-all-1.5.7.jar
SET ETLCP=%ETLCP%;lib/jscreen-1.0.jar
SET ETLCP=%ETLCP%;lib/log4j-1.2.9.jar
SET ETLCP=%ETLCP%;lib/jta.jar;lib/jtds-1.2.2.jar
SET ETLCP=%ETLCP%;lib/mail.jar
SET ETLCP=%ETLCP%;lib/opencsv-1.8.jar
SET ETLCP=%ETLCP%;lib/lucene-1.4-final.jar;
SET ETLCP=%ETLCP%;lib/org.json.jar
SET ETLCP=%ETLCP%;lib/poi-3.1-FINAL-20080629.jar
SET ETLCP=%ETLCP%;lib/servlet-api-2.5-6.1.9.jar
SET ETLCP=%ETLCP%;lib/velocity-1.4.jar
SET ETLCP=%ETLCP%;lib/jwic-ecolib.jar;lib/jwic3_2_1.jar;lib/xwic-cube-webui.jar;lib/xwic-cube.jar;
SET ETLCP=%ETLCP%;lib/swt.jar;
SET ETLCP=%ETLCP%;lib/ngs-etl.jar
SET ETLCP=%ETLCP%;lib/etlgine.jar
SET ETLCP=%ETLCP%;jetty/jetty-6.1.9.jar
SET ETLCP=%ETLCP%;jetty/jetty-util-6.1.9.jar

ECHO CLASSPATH: %ETLCP%