/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.folder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.quartz.QuartzTools;
import mx.randalf.quartz.job.JobExecute;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.exception.SolrException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JMag;

/**
 * @author massi
 *
 */
public class JFolder extends JobExecute {

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(JFolder.class);

	public static String FOLDER = "Folder";

	/**
	 * 
	 */
	public JFolder() {
//		listJobs = new Vector<JobKey>();
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String msg = null;
		Folder folder = null;
		File f = null;
		FindDocument find = null;

		folder = (Folder) context.getJobDetail().getJobDataMap().get(FOLDER);
		f = new File(folder.getPath());
		if (f.exists()) {
			if (f.isDirectory()) {
				scanFolder(new File(folder.getPath()), context, folder);
				waithEndJobs(context);
				try {
					find = new FindDocument(Configuration.getValue("solr.URL"),
							Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
							Configuration.getValue("solr.collection"),
							Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
							Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
					find.optimize();
				} catch (NumberFormatException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				} catch (SolrException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				} catch (ConfigurationException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				} catch (SolrServerException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				} catch (IOException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				}finally {
					try {
						if (find != null) {
							find.close();
						}
					} catch (IOException e) {
						log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
						throw new JobExecutionException(e.getMessage(), e, false);
					}
				}
				
			} else {
				log.error("[" + QuartzTools.getName(context) + "] non risulta essere una cartella "
						+ f.getAbsolutePath());
			}
		} else {
			log.error("[" + QuartzTools.getName(context) + "] non risulta la cartella " + f.getAbsolutePath());
		}
		msg = "[" + QuartzTools.getName(context) + "] terminato regolarmente";
		return msg;
	}

	private void scanFolder(File pathScan, JobExecutionContext context, Folder folder) {
		File[] files = null;
		File file = null;
		String typeFile = null;
		Hashtable<String, Object> params = null;

		files = pathScan.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				boolean result = false;
				File fIndex = null;
				File fError = null;
				File fCert = null;
				if (pathname.isDirectory()) {
					result = true;
				} else if (pathname.isFile()) {
					if (!pathname.getName().startsWith(".") && pathname.getName().toLowerCase().endsWith(".xml")) {
						fCert = new File(pathname.getAbsolutePath()+".cert");
						if (fCert.exists()){
							fIndex = new File(pathname.getAbsolutePath() + ".elabOK");
							fError = new File(pathname.getAbsolutePath() + ".elabKO");
							if (!fIndex.exists() && !fError.exists()) {
								result = true;
							}
						}
					}
				}
				return result;
			}
		});
		for (int x = 0; x < files.length; x++) {
			if (files[x].isDirectory()) {
				scanFolder(files[x], context, folder);
			} else {
				try {
					typeFile = checkFiles(files[x]);
					if (typeFile != null) {
						if (typeFile.equals(JMag.TYPE)){
							file = files[x];
							params = new Hashtable<String, Object>();
							params.put(JMag.FILE, file);
							params.put(FOLDER, folder);
							start(context, JMag.class, 
									context.getJobDetail().getKey().getGroup() + "." + context.getJobDetail().getKey().getName(),
									file.getAbsolutePath(), "File", file.getName(), params);
						}
					}
				} catch (SchedulerException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
				} catch (FileNotFoundException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
				} catch (IOException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * 
	 * @param fXml
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private String checkFiles(File fXml) throws FileNotFoundException, IOException {
		String type = null;
		FileReader fr = null;
		BufferedReader br = null;
		String testo = null;

		try {
			fr = new FileReader(fXml);
			br = new BufferedReader(fr);
			testo = br.readLine();
			if (testo != null) {
				if (testo.indexOf("<metadigit ") > -1 || testo.indexOf("<metadigit>") > -1) {
					type = JMag.TYPE;
				}
				if (type == null) {
					testo = br.readLine();
					if (testo != null) {
						if (testo.indexOf("<metadigit ") > -1 || testo.indexOf("<metadigit>") > -1) {
							type = JMag.TYPE;
						}
					}
				}
			}
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (br != null) {
					br.close();
				}
				if (fr != null) {
					fr.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
		return type;
	}
}
