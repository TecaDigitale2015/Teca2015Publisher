/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import mx.teca2015.tecaPublished.standAlone.quartz.folder.Folder;

/**
 * @author massi
 *
 */
class ReadParameter {
	private Integer folderTimeScheduler = null;
	private Hashtable<String, Folder> folders = null;

	/**
	 * Classe utilizzata per leggere il file dei paramentri
	 * 
	 * @param fileProp
	 * @throws FileNotFoundException
	 * @throws InvalidPropertiesFormatException
	 * @throws IOException
	 */
	public ReadParameter(String fileProp) throws FileNotFoundException,
			InvalidPropertiesFormatException, IOException {
		FileInputStream fis = null;
		File f = null;
		Properties prop = null;
		Enumeration<Object> keys = null;
		String key = null;
		String[] st = null;
		Folder folder = null;

		try {
			f = new File(fileProp);
			if (f.exists()) {
				prop = new Properties();
				fis = new FileInputStream(f);
				prop.loadFromXML(fis);
			} else {
				throw new FileNotFoundException(
						"Non risulta presente il file [" + f.getAbsolutePath()
								+ "]");
			}
			keys = prop.keys();
			folders = new Hashtable<String, Folder>();
			while (keys.hasMoreElements()) {
				key = (String) keys.nextElement();
				if (key.equals("folder.timeScheduler")) {
					folderTimeScheduler = new Integer(prop.getProperty(key));
				} else if (key.startsWith("folder.") &&
						!key.equals("folder.numberThread") &&
						!key.equals("folder.sleep")) {
					st = key.split("\\.");
					if (folders.get(st[1]) != null) {
						folder = folders.get(st[1]);
					} else {
						folder = new Folder();
					}
					if (st[2].equals("name")) {
						folder.setName(prop.getProperty(key));
					} else if (st[2].equals("path")) {
						folder.setPath(prop.getProperty(key));
					} else if (st[2].equals("pathIndex")) {
						folder.setPathIndex(prop.getProperty(key));
					} else if (st[2].equals("fondo")) {
						folder.setFondo(prop.getProperty(key));
					} else if (st[2].equals("tipoRisorsa")) {
						folder.setTipoRisorsa(prop.getProperty(key));
					}
					folders.put(st[1], folder);
				}
			}
		} catch (FileNotFoundException e) {
			throw e;
		} catch (InvalidPropertiesFormatException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}

	}

	/**
	 * @return the folderTimeScheduler
	 */
	public Integer getFolderTimeScheduler() {
		return folderTimeScheduler;
	}

	/**
	 * @return the folders
	 */
	public Hashtable<String, Folder> getFolders() {
		return folders;
	}

}
