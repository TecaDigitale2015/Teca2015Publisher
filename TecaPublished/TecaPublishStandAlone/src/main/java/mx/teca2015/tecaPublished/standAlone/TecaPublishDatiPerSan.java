/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.InvalidPropertiesFormatException;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.quartz.QuartzTools;
import mx.teca2015.tecaPublished.standAlone.quartz.san.JIstituto;
import mx.teca2015.tecaPublished.standAlone.quartz.san.SoggettoConservatore;

/**
 * @author massi
 *
 */
public class TecaPublishDatiPerSan {

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(TecaPublishDatiPerSan.class);

	/**
	 * Variabile utilizzata per la gestione della schedurlazione delle attività
	 */
	private Scheduler scheduler = null;

	private Hashtable<String, JobKey> jFolder = null;

	private Vector<SoggettoConservatore> soggettiConservatori = null;

	private Hashtable<String, String[]> compArch = null;
	private Hashtable<String, Hashtable<String, String[]>> compArchFigli = null;

	//	private ReadParameter parameter = null;

	/**
	 * @throws FileNotFoundException
	 * @throws InvalidPropertiesFormatException
	 * @throws IOException
	 * @throws SchedulerException
	 * @throws ConfigurationException
	 * 
	 */
	public TecaPublishDatiPerSan(String fileProp) throws FileNotFoundException,
			InvalidPropertiesFormatException, IOException, SchedulerException,
			ConfigurationException {
		File f = null;
		try {
			f = new File(fileProp);
			Configuration.init((f.getParentFile()==null?"./":f.getParentFile().getAbsolutePath()));
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.start();

			initSC(initCA(), initSP());

			jFolder = new Hashtable<String, JobKey>();
		} catch (SchedulerException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		}
	}

	private Hashtable<String, String[]> initSP() throws FileNotFoundException, ConfigurationException, IOException{
		File f =  null;
		BufferedReader br = null;
		FileReader fr = null;
		String line = null;
		String[] st = null;
		Hashtable<String, String[]> result = null;

		try {
			f = new File(Configuration.getValue("pathTsv"));
			if (f.exists()){
				f = new File(f.getAbsolutePath()+File.separator+Configuration.getValue("fileSoggettoProduttore"));
				if (f.exists()){
					fr = new FileReader(f);
					br = new BufferedReader(fr);
					br.readLine();
					while((line = br.readLine()) != null){
						st = line.split("\t");
						if (st.length>0 && !st[0].trim().equals("")){
							if (result == null){
								result = new Hashtable<String, String[]>();
							}
							if (result.get(st[0])== null){
								result.put(st[0], st);
							}
						}
					}
				} else {
					throw new FileNotFoundException("Il file ["+f.getAbsolutePath()+"] non esiste");
				}
			} else {
				throw new FileNotFoundException("La cartella ["+f.getAbsolutePath()+"] non esiste");
			}
		} catch (FileNotFoundException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (br != null){
					br.close();
				}
				if (fr != null){
					fr.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
		return result;
	}

	private Hashtable<String, Vector<String[]>> initCA() throws FileNotFoundException, ConfigurationException, IOException{
		File f =  null;
		BufferedReader br = null;
		FileReader fr = null;
		String line = null;
		String[] st = null;
		Hashtable<String, Vector<String[]>> result = null;

		try {
			f = new File(Configuration.getValue("pathTsv"));
			if (f.exists()){
				f = new File(f.getAbsolutePath()+File.separator+Configuration.getValue("fileComplessoArchivistico"));
				if (f.exists()){
					fr = new FileReader(f);
					br = new BufferedReader(fr);
					br.readLine();
					while((line = br.readLine()) != null){
						st = line.split("\t");
						if (st.length>0 && !st[0].trim().equals("")){
							if (compArch == null){
								compArch = new Hashtable<String, String[]>();
							}
							if (compArch.get(st[0])== null){
								compArch.put(st[0], st);
							}

							if (st.length<11 ||
									st[10] == null ||
									st[10].trim().equals("")){
								if (result == null){
									result = new Hashtable<String, Vector<String[]>>();
								}
								// Ricerca per Istituto
								if (result.get(st[9])== null){
									result.put(st[9], new Vector<String[]>());
								}
								result.get(st[9]).add(st);
							} else {
								if (compArchFigli == null){
									compArchFigli = new Hashtable<String, Hashtable<String, String[]>>();
								}
								if (compArchFigli.get(st[10])== null){
									compArchFigli.put(st[10], new Hashtable<String, String[]>());
								}
								compArchFigli.get(st[10]).put(st[0], st);
							}
						}
					}
				} else {
					throw new FileNotFoundException("Il file ["+f.getAbsolutePath()+"] non esiste");
				}
			} else {
				throw new FileNotFoundException("La cartella ["+f.getAbsolutePath()+"] non esiste");
			}
		} catch (FileNotFoundException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (br != null){
					br.close();
				}
				if (fr != null){
					fr.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
		return result;
	}

	private void initSC(Hashtable<String, Vector<String[]>> complessiArchivistici,
			Hashtable<String, String[]> soggettoProduttore) throws FileNotFoundException, ConfigurationException, IOException{
		File f =  null;
		BufferedReader br = null;
		FileReader fr = null;
		String line = null;
		String[] st = null;

		try {
			f = new File(Configuration.getValue("pathTsv"));
			if (f.exists()){
				f = new File(f.getAbsolutePath()+File.separator+Configuration.getValue("fileSoggettoConservatore"));
				if (f.exists()){
					fr = new FileReader(f);
					br = new BufferedReader(fr);
					br.readLine();
					while((line = br.readLine()) != null){
						st = line.split("\t");
						if (st.length>0 && !st[0].trim().equals("")){
							if (soggettiConservatori == null){
								soggettiConservatori = new Vector<SoggettoConservatore>();
							}
							soggettiConservatori.add(new SoggettoConservatore(st, 
									complessiArchivistici, 
									compArch, 
									compArchFigli, 
									soggettoProduttore));
						}
					}
				} else {
					throw new FileNotFoundException("Il file ["+f.getAbsolutePath()+"] non esiste");
				}
			} else {
				throw new FileNotFoundException("La cartella ["+f.getAbsolutePath()+"] non esiste");
			}
		} catch (FileNotFoundException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (br != null){
					br.close();
				}
				if (fr != null){
					fr.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TecaPublishDatiPerSan publish = null;

		try {
			if (args.length == 1) {
				publish = new TecaPublishDatiPerSan(args[0]);
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

	public void close(){
		try {
			scheduler.shutdown(true);
		} catch (SchedulerException e) {
			log.error(e.getMessage(), e);
		}
	}

	public boolean checkExecute() {
		boolean result = false;

//		try {
//			istituti = (Vector<String>) Configuration.getValues("istituti");
//			for (int x=0; x<istituti.size(); x++){
			for (int x=0; x<soggettiConservatori.size(); x++){
				try {
					if (jFolder== null){
						result = true;
					} else if (scheduler==null){
						result = true;
					} else {
						if (!(jFolder.get(soggettiConservatori.get(x).getId()) == null
							|| !scheduler.checkExists(jFolder.get(soggettiConservatori.get(x).getId())))) {
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

	public void esegui()  {
//		Vector<String> istituti = null;

//			istituti = (Vector<String>) Configuration.getValues("istituti");
//			for (int x=0; x<istituti.size(); x++){
		for (int x=0; x<soggettiConservatori.size(); x++){
			try {
//					if (jFolder.get(istituti.get(x)) == null
//							|| !scheduler.checkExists(jFolder.get(istituti.get(x)))) {
//						jFolder.put(istituti.get(x), start(istituti.get(x)));
				if (jFolder.get(soggettiConservatori.get(x).getId()) == null
						|| !scheduler.checkExists(jFolder.get(soggettiConservatori.get(x).getId()))) {
					jFolder.put(soggettiConservatori.get(x).getId(), start(soggettiConservatori.get(x)));
				}
			} catch (SchedulerException e) {
				log.error(e.getMessage(), e);
			}
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
	private JobKey start(SoggettoConservatore soggettoConservatore) throws SchedulerException {
		JobKey jobKey = null;
		Hashtable<String, Object> params = null;

		try {

			log.info("Schedulo il Job: Istituto." + soggettoConservatore.getId());
			params = new Hashtable<String, Object>();
			params.put(JIstituto.SOGGETTOCONSERVATORE, soggettoConservatore);
			jobKey = QuartzTools.startJob(scheduler, JIstituto.class, "SoggettoConservatore",
					soggettoConservatore.getId(), "SoggettoConservaotre", soggettoConservatore.getId(), 
					params);
		} catch (SchedulerException e) {
			throw e;
		}
		return jobKey;
	}

}
