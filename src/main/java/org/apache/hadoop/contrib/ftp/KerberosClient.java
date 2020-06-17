package org.apache.hadoop.contrib.ftp;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.security.UserGroupInformation;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosClient {
	
	private final Logger log = LoggerFactory.getLogger(KerberosClient.class);

	private String krb5Path = null;
	private String realm = null;
	private String kdc = null;
	private String servicePrincipalName = null;
	private Boolean krb5Debug = false;
	private Oid krb5Oid;

	private Subject subject;
	
	
	public void setKrb5Location(String krb5)
	{
		this.krb5Path = krb5;
	}

	public void setRealm(String realm) {
		this.realm = realm;
	}

	public void setKdc(String kdc) {
		this.kdc = kdc;
	}
	
	public void setKrb5Debug(Boolean krb5Debug) {
		this.krb5Debug = krb5Debug;
	}

	public void setServicePrincipalName(String servicePrincipalName) {
		this.servicePrincipalName = servicePrincipalName;
	}
	
	private void setSysProperty(String name, String value)
	{
		log.info("Setting system property "+name+"="+value);
		System.setProperty(name,value);
	}
	
	public void connectKerberos(String username, String password) throws LoginException {
		try {
			setSysProperty("sun.security.krb5.debug", krb5Debug==true?"true":"false");
			
			if (this.krb5Path != null && this.krb5Path.length() > 0)
			{
				setSysProperty("java.security.krb5.conf", this.krb5Path);
			}
			else if (this.kdc != null && this.kdc.length() > 0 &&
				this.realm != null && this.realm.length() > 0)
			{
				setSysProperty("java.security.krb5.realm", this.realm);
				setSysProperty("java.security.krb5.kdc", this.kdc);
			}
			
			
			setSysProperty("java.security.auth.login.config", "./jaas.conf");
			setSysProperty("javax.security.auth.useSubjectCredsOnly", "true");
			// use Kerberos V5 as the security mechanism.
			krb5Oid = new Oid("1.2.840.113554.1.2.2");
			
			// Login to the KDC.
			login(username, password);
			
			// Request service ticket.
			initiateSecurityContext(this.servicePrincipalName);
			
		} catch (GSSException e) {
			e.printStackTrace();
			System.err
					.println("There was an error during the security context initiation");
			System.exit(-1);
		} 
		
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
	}

	public KerberosClient() {
		super();
	}

	// Authenticate using JAAS.
	private void login(String username, String password) throws LoginException {
		LoginContext loginCtx = null;

		loginCtx = new LoginContext("Client", new LoginCallbackHandler(
				username, password));
		
		loginCtx.login();
		subject = loginCtx.getSubject();
	}

	public Subject getSubject() {
		return subject;
	}

	private void initiateSecurityContext(String servicePrincipalName)
			throws GSSException {
		GSSManager manager = GSSManager.getInstance();
		Oid krb5PrincipalNameType = new Oid("1.2.840.113554.1.2.2.1");
		GSSName serverName = manager.createName(servicePrincipalName, krb5PrincipalNameType);
		manager.createContext(serverName, krb5Oid, null, GSSContext.DEFAULT_LIFETIME);
		
	}
}