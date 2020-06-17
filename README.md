hdfs-over-ftp with kerberos authentication
==========================================
FTP server which works on a top of HDFS Source code is provided under MIT License

FTP server is configurable by hdfs-over-ftp.properties and hdfs-over-ftp-test.properties (for tests).

### Installation and running
1. Download and install java, maven
2. clone project from git  
```
git clone ...
```
3. Build package  
```
cd hdfs-over-ftp/  
mvn package -DskipTests
```
4. Copy hdfs-over-ftp.properties.template to hdfs-over-ftp.properties  
```
cp hdfs-over-ftp.properties.template hdfs-over-ftp.properties
```  
Set connection properties in hdfs-over-ftp.properties.  
5. Start server using  
```
chmod +x hdfs-over-ftp.sh
./hdfs-over-ftp.sh
```

## Build with tests
1. Copy hdfs-over-ftp-test.properties.template to hdfs-over-ftp-test.properties  
```
cp hdfs-over-ftp-test.properties.template hdfs-over-ftp-test.properties
```  
Set connection properties for tests in hdfs-over-ftp-test.properties
2. Build package with tests  
```
mvn package
```
Tests are running in an windows system with an IDE as well.

#### Config files
##### hdfs-over-ftp.properties
* **port**  
	Main FTP port of the FTP server
* **data-ports**  
	Data ports used by the FTP server
* **ssl-port**  
	Main FTP SSL port of the FTP server
* **ssl-data-ports**  
	SSL Data ports used by the FTP server
* **keystore**  
	Location of JKS keystore file
* **keystore-pass**  
	Password for the keystore file
* **core-site-location**  
	location of the core-site.xml file (e.g. /etc/hadoop/conf/core-site.xml)
* **hdfs-site-location**  
	location of the hdfs-site.xml file (e.g. /etc/hadoop/conf/hdfs-site.xml)
* **realm**  
	Kerberos realm (e.g. EXAMPLE.COM)
* **kdc**  
	Key Distribution Center used for the kerberos authentication  
	e.g. the ip or hostname of your windows Active Directory server
* **krb5-location**  
	location of the krb5.conf or krb5.ini file. Only needed if it is not in one of the default locations
* **krb5-debug**  
	enable krb5 debugging

##### hdfs-over-ftp-test.properties
Can overwrite all properties of the hdfs-over-ftp.properties file for specific values when running the tests
* **test-user**  
	User used for the tests. User must be listed in the kdc
* **test-password**  
	Password of the test user
* **test-hdfs-dir**  
	Directory in HDFS which is used to do all the tests like: creating, deleting, writing .. files and folders
* **access-control-system-used**  
	Some file/folder access tests can behave different when e.g. Ranger or Sentry is used.  
	If set to true these tests will be skipped.