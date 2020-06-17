package org.apache.hadoop.contrib.ftp;

import java.util.ArrayList;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.ftpserver.FtpServerConfigurationException;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.AbstractUserManager;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.ftpserver.util.BaseProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extended AbstractUserManager to use  HdfsUser
 */
public class KerberosUserManager extends AbstractUserManager {
	
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory
			.getLogger(KerberosUserManager.class);

	private final static String PREFIX = "ftpserver.user.";
	private BaseProperties userDataProp;
	private boolean isConfigured = false;

	
	public KerberosUserManager(String adminName) {
		
		super(adminName, new Md5PasswordEncryptor());
		// TODO Auto-generated constructor stub
	}
	
	
	/**
	 * Lazy init the user manager
	 */
	private void lazyInit() {
		if (!isConfigured) {
			configure();
		}
	}

	/**
	 * Configure user manager.
	 */
	public void configure() {
		isConfigured = true;
		try {
			userDataProp = new BaseProperties();

			
		} catch (Exception e) {
			throw new FtpServerConfigurationException(
					"Error loading user data file : ", e);
		}

	}


	/**
	 * Load user data.
	 */
	public synchronized User getUserByName(String userName) {
		lazyInit();
		
		String baseKey = PREFIX + userName + '.';
		HdfsUser user = new HdfsUser();
		user.setName(userName);
		user.setEnabled(userDataProp.getBoolean(baseKey + ATTR_ENABLE, true));
		user.setHomeDirectory("/user/"+userName+"/");
		//user.setGroups(parseGroups(userDataProp
		//		.getProperty(baseKey + "groups")));

		List<Authority> authorities = new ArrayList<Authority>();

		if (userDataProp.getBoolean(baseKey + ATTR_WRITE_PERM, false)) {
			authorities.add(new WritePermission());
		}

		int maxLogin = userDataProp.getInteger(baseKey + ATTR_MAX_LOGIN_NUMBER,
				0);
		int maxLoginPerIP = userDataProp.getInteger(baseKey
				+ ATTR_MAX_LOGIN_PER_IP, 0);

		authorities.add(new ConcurrentLoginPermission(maxLogin, maxLoginPerIP));

		int uploadRate = userDataProp.getInteger(
				baseKey + ATTR_MAX_UPLOAD_RATE, 0);
		int downloadRate = userDataProp.getInteger(baseKey
				+ ATTR_MAX_DOWNLOAD_RATE, 0);

		authorities.add(new TransferRatePermission(downloadRate, uploadRate));

		user.setAuthorities(authorities.toArray(new Authority[0]));

		user.setMaxIdleTime(userDataProp.getInteger(baseKey
				+ ATTR_MAX_IDLE_TIME, 0));

		return user;
	}

	

	/**
	 * User authenticate method
	 */
	public synchronized User authenticate(Authentication authentication)
			throws AuthenticationFailedException {
		lazyInit();
		

		if (authentication instanceof UsernamePasswordAuthentication) {
			UsernamePasswordAuthentication upauth = (UsernamePasswordAuthentication) authentication;

			String user = upauth.getUsername();
			String password = upauth.getPassword();
			
			HdfsUser loginUser = null;
			
			try 
			{
				KerberosClient krbClient = new KerberosClient();
				
				krbClient.setKrb5Location(FTPConfig.getKrb5ConfLocation());
				krbClient.setRealm(FTPConfig.getKerberosRealm() );
				krbClient.setKdc(FTPConfig.getKerberosKDC() );
				krbClient.setKrb5Debug(FTPConfig.getKrb5Debug() );
				krbClient.setServicePrincipalName(FTPConfig.getKerberosServicePrincipal() );
				
				krbClient.connectKerberos(user, password);
				
				loginUser = (HdfsUser)getUserByName(user); 
				loginUser.setKerberosSubject(krbClient.getSubject());
				
			}
			catch (LoginException e)
			{
				log.error("Authentication failed", e);
				throw new AuthenticationFailedException("Authentication failed");
			}
			
			
			
			return loginUser;
			
		}
		
		else 
		{
			return null;
		}
		
	}

	/**
	 * Close the user manager - remove existing entries.
	 */
	public synchronized void dispose() {
		if (userDataProp != null) {
			userDataProp.clear();
			userDataProp = null;
		}
	}


	@Override
	public void delete(String username) throws FtpException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void save(User user) throws FtpException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getAllUserNames() throws FtpException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean doesExist(String username) throws FtpException {
		// TODO Auto-generated method stub
		return false;
	}
}