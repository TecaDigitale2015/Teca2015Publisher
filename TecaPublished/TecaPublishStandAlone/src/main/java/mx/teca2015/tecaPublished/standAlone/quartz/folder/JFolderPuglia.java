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
import java.util.UUID;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import mx.randalf.quartz.QuartzTools;
import mx.randalf.quartz.job.JobExecute;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JMag;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JSchedaF;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JUC;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JUD;

/**
 * @author massi
 *
 */
public class JFolderPuglia extends JobExecute {

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(JFolderPuglia.class);

	public static String FOLDER = "Folder";

	public static String GENMAGCOMP = "genMagComp";

	public static String GENMAGPUB = "genMagPub";

//	private Vector<JobKey> listJobs = null;

	/**
	 * 
	 */
	public JFolderPuglia() {
//		listJobs = new Vector<JobKey>();
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String msg = null;
		Folder folder = null;
		File f = null;
		boolean genMagComp = true;
		boolean genMagPub = false;

		folder = (Folder) context.getJobDetail().getJobDataMap().get(FOLDER);
		genMagComp = (Boolean) context.getJobDetail().getJobDataMap().get(GENMAGCOMP);
		genMagPub = (Boolean) context.getJobDetail().getJobDataMap().get(GENMAGPUB);
		f = new File(folder.getPath());
		if (f.exists()) {
			if (f.isDirectory()) {
				scanFolder(new File(folder.getPath()), new File(folder.getPathIndex()), context,
						new File(folder.getPath()), genMagComp, genMagPub);
				waithEndJobs(context);
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

	private void scanFolder(File folder, final File folderIndex, JobExecutionContext context, File folderOri,
			final boolean genMagComp, final boolean genMagPub) {
		File[] files = null;
		String typeFile = null;

		files = folder.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				boolean result = false;
				File elabOK = null;
				File elabKO = null;
				File[] fl = null;
				// File elabOK2 = null;
				// File elabKO2 = null;
				if (pathname.isDirectory()) {
					result = true;
				} else if (pathname.isFile()) {
					if (!pathname.getName().startsWith(".")) {
						if (pathname.getName().endsWith(".xls") && !pathname.getName().endsWith("_Nomenclatura.xls")) {
							if (genMagPub) {
								elabOK = new File(pathname.getAbsolutePath() + ".elabOK");
								elabKO = new File(pathname.getAbsolutePath() + ".elabKO");
							} else if (genMagComp) {
								elabOK = new File(pathname.getParentFile().getAbsolutePath() + File.separator
										+ pathname.getName().replace(".xls", "_mag.xml.cert"));
							}
							// elabOK2 = new
							// File(folderIndex.getAbsolutePath()+File.separator+
							// pathname.getName().replace(" ",
							// "_").replace(".xls", "_mag.xml")+".elabOK");
							// elabKO2 = new
							// File(folderIndex.getAbsolutePath()+File.separator+
							// pathname.getName().replace(" ",
							// "_").replace(".xls", "_mag.xml")+".elabKO");
							if (!elabOK.exists() && (elabKO == null || !elabKO.exists())) {
								result = true;
							}
						} else if (pathname.getName().equals("SS114F.xml")) {
							if (genMagPub) {
								elabOK = new File(pathname.getAbsolutePath() + ".elabOK");
								elabKO = new File(pathname.getAbsolutePath() + ".elabKO");
								if (!elabOK.exists() && !elabKO.exists()) {
									result = true;
								}
							} else if (genMagComp) {
								elabOK = new File(
										pathname.getParentFile().getAbsolutePath() + File.separator + "*_mag.xml.cert");
								fl = pathname.getParentFile().listFiles(new FileFilter() {

									@Override
									public boolean accept(File pathname) {
										boolean result = false;
										if (!pathname.isDirectory()) {
											if (!pathname.getName().startsWith(".")) {
												if (pathname.getName().endsWith("_mag.xml.cert")) {
													result = true;
												}
											}
										}
										return result;
									}
								});
								if (fl != null && fl.length > 0) {
									result = false;
								} else {
									elabKO = new File(pathname.getAbsolutePath() + ".elabKO");
									if (elabKO.exists()) {
										result = false;
									} else {
										result = true;
									}
								}
							}
						}
					}
				}
				return result;
			}
		});
		for (int x = 0; x < files.length; x++) {
			if (files[x].isDirectory()) {
				scanFolder(files[x], new File(folderIndex.getAbsolutePath() + File.separator + files[x].getName()),
						context, folderOri, genMagComp, genMagPub);
			} else {
				try {
					typeFile = checkFiles(files[x]);
					if (typeFile != null) {
//						checkJobs(context);
						start(files[x], folderIndex, context, typeFile, folderOri, genMagComp, genMagPub);
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
		String[] st = null;
		String testo = null;

		try {
			// System.out.println("File: "+fXml.getAbsolutePath());
			st = fXml.getName().replace(".xls", "").split("_");
			if (st[st.length - 1].startsWith("UC")) {
				type = JUC.TYPE;
			} else if (st[st.length - 1].startsWith("UD")) {
				type = JUD.TYPE;
			} else {
				fr = new FileReader(fXml);
				br = new BufferedReader(fr);
				testo = br.readLine();
				if (testo != null) {
					if (testo.indexOf("<csm_root ") > -1 || testo.indexOf("<csm_root>") > -1) {
						type = JSchedaF.TYPE;
					}
					if (type == null) {
						testo = br.readLine();
						if (testo != null) {
							if (testo.indexOf("<csm_root ") > -1 || testo.indexOf("<csm_root>") > -1) {
								type = JSchedaF.TYPE;
							}
						}
					}
				}
			}
			if (type == null) {
				System.out.println("non so");
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

//	private void checkJobs(JobExecutionContext context) {
//		int numberThread = 10;
//		int sleep = 5000;
//
//		try {
//			if (Configuration.getValue("folder.numberThread") != null) {
//				numberThread = Integer.parseInt(Configuration.getValue("folder.numberThread"));
//			}
//
//			if (Configuration.getValue("folder.sleep") != null) {
//				sleep = Integer.parseInt(Configuration.getValue("folder.sleep"));
//			}
//		} catch (NumberFormatException e) {
//			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
//		} catch (ConfigurationException e) {
//			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
//		}
//		while (true) {
//			for (int x = 0; x < listJobs.size(); x++) {
//				try {
//					if (!context.getScheduler().checkExists(listJobs.get(x))) {
//						listJobs.remove(x);
//					}
//				} catch (SchedulerException e) {
//					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
//				}
//			}
//			if (listJobs.size() < numberThread) {
//				break;
//			} else {
//				try {
//					Thread.sleep(sleep);
//				} catch (InterruptedException e) {
//					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
//				}
//			}
//		}
//	}

//	private void waithEndJobs(JobExecutionContext context) {
//		while (true) {
//			for (int x = 0; x < listJobs.size(); x++) {
//				try {
//					if (!context.getScheduler().checkExists(listJobs.get(x))) {
//						listJobs.remove(x);
//					}
//				} catch (SchedulerException e) {
//					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
//				}
//			}
//			if (listJobs.size() == 0) {
//				break;
//			} else {
//				try {
//					Thread.sleep(10000);
//				} catch (InterruptedException e) {
//					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
//				}
//			}
//		}
//	}

	@SuppressWarnings("unchecked")
	private void start(File file, File folderIndex, JobExecutionContext context, String typeFile, File folderOri,
			boolean genMagComp, boolean genMagPub) throws SchedulerException {
		Hashtable<String, Object> params = null;
		Class<?> myClass = null;

		try {
			log.info("[" + QuartzTools.getName(context) + "] Schedulo il Job: File." + file.getAbsolutePath());
			params = new Hashtable<String, Object>();
			params.put(JMag.FILE, file);
			params.put(JMag.FOLDERINDEX, folderIndex);
			params.put(JMag.FOLDERORI, folderOri);
			params.put(JMag.GENMAGCOMP, genMagComp);
			params.put(JMag.GENMAGPUB, genMagPub);
			if (typeFile.equals(JMag.TYPE)) {
				myClass = JMag.class;
			} else if (typeFile.equals(JUC.TYPE)) {
				myClass = JUC.class;
			} else if (typeFile.equals(JUD.TYPE)) {
				myClass = JUD.class;
			} else if (typeFile.equals(JSchedaF.TYPE)) {
				myClass = JSchedaF.class;
			}
			if (myClass != null) {
				start(context, (Class<? extends JobExecute>) myClass,
						context.getJobDetail().getKey().getGroup() + "." + context.getJobDetail().getKey().getName(),
						file.getAbsolutePath(), "File", file.getName() + " - " + UUID.randomUUID().toString(), params);
			} else {
				log.error("Non è possibile eseguire la verifica per il tipo File [" + typeFile + "]");
			}
		} catch (SchedulerException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw e;
		}
	}
}
