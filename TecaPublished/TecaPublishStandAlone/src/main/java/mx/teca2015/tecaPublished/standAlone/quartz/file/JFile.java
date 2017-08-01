/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.quartz.QuartzTools;
import mx.randalf.quartz.job.JobExecute;
import mx.randalf.solr.exception.SolrException;
import mx.randalf.solr.tools.JTools;
import mx.teca2015.tecaPublished.standAlone.quartz.file.exception.JFileException;
import mx.teca2015.tecaPublished.standAlone.quartz.san.ComplessoArchivistico;
import mx.teca2015.tecaPublished.standAlone.quartz.san.JFondo;
import mx.teca2015.tecaPublished.standAlone.quartz.san.JSoggettoProduttore;
import mx.teca2015.tecaPublished.standAlone.quartz.san.SoggettoProduttore;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public abstract class JFile<M, X> extends JTools {

	public static String FILE="file";

	public static String FOLDERINDEX="folderIndex";

	public static String FOLDERORI="folderOri";

	public static String GENMAGCOMP="genMagComp";

	public static String GENMAGPUB="genMagPub";

	private Logger log = Logger.getLogger(JFile.class);

	/**
	 * 
	 */
	public JFile() {
	}

	protected abstract void publisher(String objectIdentifier, String filename, M md, X mdXsd, IndexDocumentTeca admd) throws SolrException, FileNotFoundException;

	public static boolean convertImg(File imgInput, File imgOutput) throws JFileException {
		Runtime runtime = null;
		String[] cmdarray = null;
		Process process = null;
		Integer processResult = null;
		String line = null; 
		BufferedReader stdErr = null;
		Vector<String> err = new Vector<String>();
		BufferedReader stdOut = null;
		Vector<String> out = new Vector<String>();
		boolean result = true;
		String cmd = null;

		try {
			runtime = Runtime.getRuntime();

			if (imgInput.exists()){
				if (!imgOutput.getParentFile().exists()){
					if (!imgOutput.getParentFile().mkdirs()){
						throw new JFileException("Impossibile creare la cartella ["+imgInput.getAbsolutePath()+"]");
					}
				}
				cmdarray = new String[4];
				cmdarray[0] = Configuration.getValue("convertImg.batch");
				cmdarray[1] = imgInput.getAbsolutePath();
				cmdarray[2] = imgOutput.getAbsolutePath();
				cmdarray[3] = Configuration.getValue("convertImg.pathTmp")+UUID.randomUUID().toString();

				cmd = "";
				for (int x=0;x<cmdarray.length; x++){
					cmd += (cmd.equals("")?"":" ")+cmdarray[x];
				}
				
				System.out.println("CMD: "+cmd);
				process = runtime.exec(cmd);
				stdErr = new BufferedReader(new InputStreamReader(
						process.getErrorStream()));
				stdOut = new BufferedReader(new InputStreamReader(
						process.getInputStream()));
	
				while ((line = stdOut.readLine()) != null) {
					if (line.toLowerCase().indexOf("fatal")>-1 ||
							line.toLowerCase().indexOf("error")>-1 ||
							line.toLowerCase().indexOf("warning")>-1
							){
						result = false;
					}
					out.add(line);
				}
	
				while ((line = stdErr.readLine()) != null) {
//					result = false;
					System.out.println("ERROR: "+line);
					err.add(line);
				}
				processResult = process.waitFor();
				if (processResult !=0){
					result = false;
				}
				if (!imgOutput.exists() || imgOutput.length()==0){
					result = false;
				}
			} else {
				throw new JFileException("Il file ["+imgInput.getAbsolutePath()+"] non esiste");
			}
		} catch (IOException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (InterruptedException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (ConfigurationException e) {
			throw new JFileException(e.getMessage(), e);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected void start (ComplessoArchivistico complessoArchivistico, String idSoggettoConservatore,
			String soggettoConservatore, JobExecutionContext context) throws SchedulerException{
		Hashtable<String, Object> params = null;
		Class<?> myClass = null;

		try {
			log.info("["+QuartzTools.getName(context)+"] Schedulo il Job: Fondo."+complessoArchivistico.getId());
			params = new Hashtable<String, Object>();
			params.put(JFondo.COMPLESSOARCHIVISTICO, complessoArchivistico);
			params.put(JFondo.IDSOGETTOCONSERVATORE, idSoggettoConservatore);
			params.put(JFondo.SOGGETTOCONVERVATORE, soggettoConservatore);
			myClass = JFondo.class;

			start(context, 
					(Class<? extends JobExecute>) myClass, 
					context.getJobDetail().getKey().getGroup()+"."+ 
					context.getJobDetail().getKey().getName(), 
					complessoArchivistico.getId(),
					"Fondo",
					complessoArchivistico.getId()+" - "+UUID.randomUUID().toString(),
					params);
		} catch (SchedulerException e) {
			log.error("["+QuartzTools.getName(context)+"] "+e.getMessage(), e);
			throw e;
		}
	}

	protected String findId(JobExecutionContext context, String id) throws JobExecutionException{
		return findId(context, ItemTeca.BID, id, ItemTeca.ID);
	}
	
	@SuppressWarnings("unchecked")
	protected void start (SoggettoProduttore soggettoProduttore, String idComplesoArchivistico,
			String complessoArchivistico, JobExecutionContext context) throws SchedulerException{
//		JobKey jobKey = null;
		Hashtable<String, Object> params = null;
		Class<?> myClass = null;

		try {
			log.info("["+QuartzTools.getName(context)+"] Schedulo il Job: SoggettoProduttore."+soggettoProduttore.getId());
			params = new Hashtable<String, Object>();
			params.put(JSoggettoProduttore.SOGGETTOPRODUTTORE, soggettoProduttore);
			params.put(JSoggettoProduttore.IDCOMPLESSOARCHIVISTICO, idComplesoArchivistico);
			params.put(JSoggettoProduttore.COMPLESSOARCHIVISTICO, complessoArchivistico);
			myClass = JSoggettoProduttore.class;

			start(context, (Class<? extends JobExecute>) myClass, 
					context.getJobDetail().getKey().getGroup()+"."+ 
					context.getJobDetail().getKey().getName(), 
					soggettoProduttore.getId(), 
					"SoggettoProduttore", 
					soggettoProduttore.getId()+" - "+UUID.randomUUID().toString(), 
					params);
		} catch (SchedulerException e) {
			log.error("["+QuartzTools.getName(context)+"] "+e.getMessage(), e);
			throw e;
		}
//		return jobKey;
	}

}
