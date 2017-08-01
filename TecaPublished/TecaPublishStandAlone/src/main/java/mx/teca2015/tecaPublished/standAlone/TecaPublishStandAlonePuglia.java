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
import org.quartz.JobKey;
import org.quartz.SchedulerException;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.quartz.QuartzExecute;
import mx.teca2015.tecaPublished.standAlone.quartz.folder.Folder;
import mx.teca2015.tecaPublished.standAlone.quartz.folder.JFolderPuglia;

/**
 * @author massi
 *
 */
public class TecaPublishStandAlonePuglia 
extends QuartzExecute 
{

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(TecaPublishStandAlonePuglia.class);

	private ReadParameter parameter = null;

	/**
	 * @throws FileNotFoundException
	 * @throws InvalidPropertiesFormatException
	 * @throws IOException
	 * @throws SchedulerException
	 * @throws ConfigurationException
	 * 
	 */
	public TecaPublishStandAlonePuglia(String fileProp) throws FileNotFoundException,
			InvalidPropertiesFormatException, IOException, SchedulerException,
			ConfigurationException {
		super(fileProp);
		File f = null;
		try {
			f = new File(fileProp);
			parameter = new ReadParameter(f.getAbsolutePath());
		} catch (FileNotFoundException e) {
			throw e;
		} catch (InvalidPropertiesFormatException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TecaPublishStandAlonePuglia publish = null;

		try {
			if (args.length == 1) {
				publish = new TecaPublishStandAlonePuglia(args[0]);
//				System.out.println("Inizio Pubblicazione");
				log.info("Inizio pubblicazione");
				publish.esegui();
//				System.out.println("Eseguita la pubblicazione inizio la verifica per la conclusione");
				while (true) {
//					System.out.println("Verifico lo status");
					if (!publish.checkExecute()) {
						break;
					}
					try {
//						System.out.println("Rimango in attesa per 10 min");
						Thread.sleep(600000);
					} catch (InterruptedException e) {
						log.error(e.getMessage(), e);
					}
				}
				publish.close();
//				System.out.println("Fine Pubblicazione");
				log.info("Fine pubblicazione");
			} else {
				System.out
						.println("E' ncessario indicare il file di configurazione");
			}
		} catch (FileNotFoundException e) {
			log.error(e.getMessage(), e);
		} catch (InvalidPropertiesFormatException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} catch (SchedulerException e) {
			log.error(e.getMessage(), e);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(), e);
		}
	}

	public boolean checkExecute(){
		Enumeration<String> keys = null;
		String key = null;
		keys = parameter.getFolders().keys();
		boolean result = false;
		while (keys.hasMoreElements()) {
			try {
				key = keys.nextElement();
//				System.out.println("key: "+key);
//				System.out.println("jFolder: "+jFolder);
//				System.out.println("scheduler: "+scheduler);
				if (jList== null){
					result = true;
				} else if (scheduler==null){
					result = true;
				} else {
//					System.out.println("jFolder.get(key): "+jFolder.get(key));
//					System.out.println("scheduler.checkExists(jFolder.get(key)): "+(jFolder.get(key)==null?"":scheduler.checkExists(jFolder.get(key))));
					if (!(jList.get(key) == null
						|| !scheduler.checkExists(jList.get(key)))) {
						result = true;
					}
				}
			} catch (SchedulerException e) {
				log.error(e.getMessage(), e);
				result = true;
			}
		}
//		System.out.println("result: "+result);
		return result;
	}

	public void esegui() throws ConfigurationException {
		Enumeration<String> keys = null;
		String key = null;
		boolean genMagComp = false;
		boolean genMagPub = false;

		try {
			if (Configuration.getValueDefault("modalita", "").equals("index")){
				genMagComp = false;
				genMagPub = true;
			} else if (Configuration.getValueDefault("modalita", "").equals("genMag")){
				genMagComp = true;
				genMagPub = false;
			} else {
				throw new ConfigurationException("Modalità di lavoro non supportata");
			}
			keys = parameter.getFolders().keys();
			while (keys.hasMoreElements()) {
				try {
					key = keys.nextElement();
					if (jList.get(key) == null
							|| !scheduler.checkExists(jList.get(key))) {
						jList.put(key, start(parameter.getFolders().get(key),genMagComp, genMagPub));
					}
				} catch (SchedulerException e) {
					log.error(e.getMessage(), e);
				}
			}
		} catch (ConfigurationException e) {
			throw e;
		}
	}

	/**
	 * Metodo utilizzato per l'esecuzione della scheduzione dell'attività
	 * relativo all'istituzione
	 * 
	 * @param mdIstituzione
	 * @return
	 * @throws SchedulerException
	 */
	protected JobKey start(Folder folder, boolean genMagComp, boolean genMagPub) throws SchedulerException {
		JobKey jobKey = null;
		Hashtable<String, Object> params = null;

		try {

			log.info("Schedulo il Job: Folder." + folder.getName());
			params = new Hashtable<String, Object>();
			params.put(JFolderPuglia.FOLDER, folder);
			params.put(JFolderPuglia.GENMAGCOMP, genMagComp);
			params.put(JFolderPuglia.GENMAGPUB, genMagPub);
//			jobKey = QuartzTools.startJob(scheduler, JFolder.class, "Folder",
//					folder.getName(), "Folder", folder.getName(), params);
			jobKey = start(JFolderPuglia.class, "Folder",
					folder.getName(), "Folder", folder.getName(), params);
		} catch (SchedulerException e) {
			throw e;
		}
		return jobKey;
	}

}
