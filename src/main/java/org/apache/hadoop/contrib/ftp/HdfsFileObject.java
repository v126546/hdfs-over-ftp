package org.apache.hadoop.contrib.ftp;

import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.WRITE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements all actions to HDFS
 */
public class HdfsFileObject implements FtpFile {

	private final Logger log = LoggerFactory.getLogger(HdfsFileObject.class);

	private Path path;
	private HdfsUser user;
	private FileStatus fs;
	//private AclStatus acl;

	/**
	 * Constructs HdfsFileObject from path
	 *
	 * @param path
	 *            path to represent object
	 * @param user
	 *            accessor of the object
	 */
	public HdfsFileObject(String path, User user) {
		this.path = new Path(path);
		this.user = (HdfsUser) user;
		FileSystem dfs;
		
		
		try {
			dfs = HdfsOverFtpSystem.getDfs(user);
			
			try {
				this.fs = dfs.getFileStatus(this.path);
				log.info("path: "+path);
			} catch (IOException e) {
				this.fs = null;
			} 
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Get full name of the object
	 *
	 * @return full name of the object
	 */
	public String getFullName() {
		return path.toString();
	}

	/**
	 * Get short name of the object
	 *
	 * @return short name of the object
	 */
	public String getShortName() {
		String full = getFullName();
		int pos = full.lastIndexOf("/");
		if (pos == 0) {
			return "/";
		}
		return full.substring(pos + 1);
	}

	/**
	 * HDFS has no hidden objects
	 *
	 * @return always false
	 */
	public boolean isHidden() {
		return false;
	}

	/**
	 * Checks if the object is a directory
	 *
	 * @return true if the object is a directory
	 */
	public boolean isDirectory() {
		try {
			//log.debug("is directory? : " + path);
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			FileStatus fs = dfs.getFileStatus(path);
			return fs.isDirectory();
		} catch (IOException e) {
			//log.debug(path + " is not dir", e);
			return false;
		}
	}

	

	/**
	 * Checks if the object is a file
	 *
	 * @return true if the object is a file
	 */
	public boolean isFile() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			return dfs.isFile(path);
		} catch (IOException e) {
			//log.debug(path + " is not file", e);
			return false;
		}
	}

	/**
	 * Checks if the object does exist
	 *
	 * @return true if the object does exist
	 */
	public boolean doesExist() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			dfs.getFileStatus(path);
			return true;
		} catch (IOException e) {
			// log.debug(path + " does not exist", e);
			return false;
		}
	}

	/**
	 * Checks if the user has a read permission on the object
	 *
	 * @return true if the user can read the object
	 */
	public boolean hasReadPermission() {
		try {
			log.info("hasReadPermission check: "+path);
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			dfs.access(this.path, READ);
			return true;
			
		} catch (AccessControlException e) {
			log.warn("hasReadPermission:",e);
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} 
	}

	private HdfsFileObject getParent() {
		
		String pathS = path.toString();
		String parentS = "/";
		int pos = pathS.lastIndexOf("/");
		if (pos > 0) {
			parentS = pathS.substring(0, pos);
		}
		return new HdfsFileObject(parentS, user);
	}

	/**
	 * Checks if the user has a write permission on the object
	 *
	 * @return true if the user has write permission on the object
	 */
	public boolean hasWritePermission() {
		
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			dfs.access(this.path, WRITE);
			return true;
			
		} catch (AccessControlException e) {
			log.warn("hasWritePermission:",e);
			return false;
		} catch (java.io.FileNotFoundException e) {
			return getParent().hasWritePermission();
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * Checks if the user has a delete permission on the object
	 *
	 * @return true if the user has delete permission on the object
	 */
	public boolean hasDeletePermission() {
		return hasWritePermission();
	}

	/**
	 * Get owner of the object
	 *
	 * @return owner of the object
	 */
	public String getOwnerName() {
		//FileSystem dfs = HdfsOverFtpSystem.getDfs();
		//FileStatus fs = dfs.getFileStatus(path);
		return fs.getOwner();
	}

	/**
	 * Get group of the object
	 *
	 * @return group of the object
	 */
	public String getGroupName() {
		//FileSystem dfs = HdfsOverFtpSystem.getDfs();
		//FileStatus fs = dfs.getFileStatus(path);
		return fs.getGroup();
	}

	/**
	 * Get link count
	 *
	 * @return 3 is for a directory and 1 is for a file
	 */
	public int getLinkCount() {
		return isDirectory() ? 3 : 1;
	}

	/**
	 * Get last modification date
	 *
	 * @return last modification date as a long
	 */
	public long getLastModified() {
		return fs.getModificationTime();
	}

	/**
	 * Get a size of the object
	 *
	 * @return size of the object in bytes
	 */
	public long getSize() {
		if (this.isDirectory())
		{
			return 0L;
		} else {
			return fs.getLen();
		}
	}
	
	/**
	 * Get a mode of the object
	 *
	 * @return size of the object in bytes
	 */
	public String getMode() {
		return String.format("%1$03o", fs.getPermission().toShort());
	}

	/**
	 * Create a new dir from the object
	 *
	 * @return true if dir is created
	 */
	public boolean mkdir() {

		if (!hasWritePermission()) {
			log.debug("No write permission : " + path);
			return false;
		}

		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			
			dfs.mkdirs(path);
			dfs.setOwner(path, user.getName(), user.getMainGroup());
			
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Delete object from the HDFS filesystem
	 *
	 * @return true if the object is deleted
	 */
	public boolean delete() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			dfs.delete(path, true);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Move the object to another location
	 *
	 * @param fileObject
	 *            location to move the object
	 * @return true if the object is moved successfully
	 */
	public boolean move(FtpFile fileObject) {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			dfs.rename(path, new Path(fileObject.getAbsolutePath()) );
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * List files of the directory
	 *
	 * @return List of files in the directory
	 */
	public List<FtpFile> listFiles() {

		if (!hasReadPermission()) {
			log.debug("No read permission : " + path);
			return null;
		}

		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			FileStatus fileStats[] = dfs.listStatus(path);

			FtpFile fileObjects[] = new FtpFile[fileStats.length];
			for (int i = 0; i < fileStats.length; i++) {
				fileObjects[i] = new HdfsFileObject(fileStats[i].getPath().toString(), user);
			}
			return Arrays.asList(fileObjects);
		} catch (IOException e) {
			log.debug("", e);
			return null;
		}
	}

	/**
	 * Creates output stream to write to the object
	 *
	 * @param l
	 *            is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public OutputStream createOutputStream(long offset) throws IOException {
		// permission check
		if (!hasWritePermission()) {
			throw new IOException("No write permission : " + path);
		}

		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			FSDataOutputStream out = null;
			if (offset == 0)
			{
				out = dfs.create(path);
			}
			else
			{
				if (offset != getSize() )
				{
					throw new IOException("File does not exists (for appending): " + path);
				}
				out = dfs.append(path);
			}
			log.info("path: " + path + " user: "+ user.getName() +"group: " + user.getMainGroup());
			dfs.setOwner(path, user.getName(), user.getMainGroup());
			return out;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates input stream to read from the object
	 *
	 * @param offset
	 *       offset to start reading at the correct position
	 * @return OutputStream
	 * @throws IOException
	 */
	public InputStream createInputStream(long offset) throws IOException {
		// permission check
		if (!hasReadPermission()) {
			throw new IOException("No read permission : " + path);
		}
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs(user);
			FSDataInputStream in = dfs.open(path);
			in.skip(offset);
			return in;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String getAbsolutePath() {
		return path.toString();
	}

	@Override
	public String getName() {

		return path.getName();
	}

	@Override
	public boolean isReadable() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean isRemovable() {
		
		return true;
	}

	@Override
	public boolean isWritable() {
		/*
		log.debug("Checking authorization for " + getAbsolutePath());
		if (user.authorize(new WriteRequest(getAbsolutePath())) == null) {
			log.debug("Not authorized");
			return false;
		}

		log.debug("Checking if file exists");
		if (this.doesExist()) {
			log.debug("Checking can write: " + this.isWritable());
			return this.isWritable();
		}
		*/
		//log.debug("Authorized");
		return true;
	}

	@Override
	public boolean setLastModified(long arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean chmod(String rights) throws AccessControlException {
		FileSystem dfs;
		try {
			dfs = HdfsOverFtpSystem.getDfs(user);
			FsPermission fp = new FsPermission(Short.decode("0"+rights));
			log.info("chmod: " + path + " to "+ fp.toString());
			dfs.setPermission(path, fp);
			return true;
		} catch (AccessControlException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		
		
	}

	@Override
	public Object getPhysicalFile() {
		// TODO Auto-generated method stub
		return null;
	}

}
