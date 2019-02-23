/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.InvalidPropertiesFormatException;

import org.apache.log4j.Logger;
import org.quartz.SchedulerException;

import it.bncf.magazziniDigitali.configuration.IConfiguration;
import it.bncf.magazziniDigitali.configuration.exception.MDConfigurationException;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.quartz.QuartzScheduler;
import mx.randalf.quartz.QuartzTools;
import mx.teca2015.tecaPublished.standAlone.quartz.folder.Folder;
import mx.teca2015.tecaPublished.standAlone.quartz.folder.JFolder;

/**
 * @author massi
 *
 */
public class TecaPublishStandAlone extends QuartzScheduler {

	public static IConfiguration configuration = null;

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(TecaPublishStandAlone.class);

	public static ReadParameter parameter = null;

	/**
	 * @throws FileNotFoundException
	 * @throws InvalidPropertiesFormatException
	 * @throws IOException
	 * @throws SchedulerException
	 * @throws ConfigurationException
	 * 
	 */
	public TecaPublishStandAlone(boolean processing, String fileQuartz, Integer socketPort, boolean closeSocket,
			boolean reScheduling) throws SchedulerException {
		super(processing, fileQuartz, socketPort, closeSocket, reScheduling, true);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TecaPublishStandAlone quartzDeskScheduler = null;
		Integer socketPort = null;
		boolean closeSocket = false;
		boolean processing = false;
		boolean scheduling = false;
		boolean rescheduling = false;
		File fConfig = null;

		try {
			if (args.length == 3) {

				fConfig = new File(args[0]);
				TecaPublishStandAlone.parameter = new ReadParameter(fConfig.getAbsolutePath());
				TecaPublishStandAlone.configuration = new IConfiguration("file:///" + fConfig.getAbsolutePath());

				if (args[2].equalsIgnoreCase("stop")) {
					closeSocket = true;
				} else if (args[2].equalsIgnoreCase("start")) {
					closeSocket = false;
				} else if (args[2].equalsIgnoreCase("rescheduling")) {
					rescheduling = true;
				} else {
					printHelp();
					System.exit(-1);
				}

				if (args[1].equalsIgnoreCase("Scheduler")){
					processing = false;
					scheduling = true;

					try {
						socketPort = TecaPublishStandAlone.configuration.getConfigInteger("socketPortScheduler");
						if (socketPort == null) {
							socketPort = 9000;
						}
					} catch (MDConfigurationException e) {
						socketPort = 9000;
					}
				} else if (args[1].equalsIgnoreCase("Processing")){
					processing = true;
					scheduling = false;

					try {
						socketPort = TecaPublishStandAlone.configuration.getConfigInteger("socketPortProcessing");
						if (socketPort == null) {
							socketPort = 9001;
						}
					} catch (MDConfigurationException e) {
						socketPort = 9001;
					}
				}

				if (processing) {
					log.info("Rimango in attesa per l'elaborazione dei Jobs schedulati\n"
							+ "Porta per la chiusura del programma: " + socketPort);
				}

				quartzDeskScheduler = new TecaPublishStandAlone(processing, fConfig.getParentFile().getAbsolutePath() + "/quartz.properties", socketPort,
						closeSocket, rescheduling);
				if (!closeSocket) {
					if (!rescheduling) {
						if (scheduling) {
							log.info("Inizio l'attività di Scheduling.\nPorta per la chiusura del programma: "
									+ socketPort);
							quartzDeskScheduler.scheduling();
						}
					} else {
						log.info("Inizio l'attività di Re-Scheduling.\nPorta per la chiusura del programma: "
								+ socketPort);
						quartzDeskScheduler.reScheduling();
						quartzDeskScheduler.scheduler.shutdown();
					}
				}

			} else {
				printHelp();
			}
		} catch (SchedulerException e) {
			log.error(e.getMessage(), e);
		} catch (MDConfigurationException e) {
			log.error(e.getMessage(), e);
		} catch (FileNotFoundException e) {
			log.error(e.getMessage(), e);
		} catch (InvalidPropertiesFormatException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}

	private static void printHelp() {
		System.out.println("E' necessario indicare i seguenti parametri:");
		System.out.println("1) File Configurazione");
		System.out.println("2) Codice identificativo del Software");
		System.out.println("3) Azione (start/stop/rescheduling)");
	}

	@Override
	protected void scheduling() throws SchedulerException {
		Enumeration<String> keys = null;
		String key = null;
		Hashtable<String, Object> params = null;
		Folder folder = null;
		while (!this.isShutdown()) {

			keys = parameter.getFolders().keys();
			while (keys.hasMoreElements()) {
				try {
					key = keys.nextElement();
					folder = parameter.getFolders().get(key);

					params = new Hashtable<String, Object>();
					params.put(JFolder.FOLDER, folder);

					add(key, QuartzTools.startJob(scheduler, JFolder.class, "Folder", folder.getName(), "Folder",
							folder.getName(), params, null, null));
				} catch (SchedulerException e) {
					log.error(e.getMessage(), e);
				}
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	@Override
	protected void reScheduling() throws SchedulerException {
	}

}
