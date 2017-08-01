/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;
import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.mag.MagNamespacePrefix;
import mx.randalf.mag.MagXsd;
import mx.randalf.quartz.QuartzTools;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.randalf.xsd.exception.XsdException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.exception.JFileException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.schede.UC;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.im4java.process.ProcessStarter;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

/**
 * @author massi
 *
 */
public class JUC extends JFile<UC, MagXsd> {

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(JUC.class);

	public static String TYPE = "uc";

	/**
	 * 
	 */
	public JUC() {
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {

		File f = null;
		File folderIndex = null;
		File fSolr = null;
		File fMagIndex = null;
		IndexDocumentTeca admd = null;
		String objectIdentifier = null;
		FindDocument find = null;
		QueryResponse qr = null;
		String query = "";
		SolrDocumentList response = null;
		Workbook wb = null;
		Sheet finestra = null;
		UC uc = null;
		boolean genMagComp = true;
		boolean genMagPub = false;
		String msg = null;

		try {
			ProcessStarter.setGlobalSearchPath(Configuration.getValue("calcImg.path"));
			f = (File) context.getJobDetail().getJobDataMap().get(FILE);
			folderIndex = (File) context.getJobDetail().getJobDataMap().get(FOLDERINDEX);

			if (context.getJobDetail().getJobDataMap().get(GENMAGCOMP) != null)
				genMagComp = (Boolean) context.getJobDetail().getJobDataMap().get(GENMAGCOMP);

			if (context.getJobDetail().getJobDataMap().get(GENMAGPUB) != null)
				genMagPub = (Boolean) context.getJobDetail().getJobDataMap().get(GENMAGPUB);

			log.info("[" + QuartzTools.getName(context) + "] Inizio la pubblicazione del file [" + f.getAbsolutePath()
					+ "]");

			// Apro il file xlsx
			wb = openFileXls(f.getAbsolutePath());

			// Leggo le infomrazioni della 1 finestra
			finestra = wb.getSheet(0);

			uc = new UC(finestra.getRow(1), f, folderIndex, genMagComp, genMagPub);

			if (genMagPub) {
				admd = new IndexDocumentTeca();

				try {
					find = new FindDocument(Configuration.getValue("solr.URL"),
							Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
							Configuration.getValue("solr.collection"),
							Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
							Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
					query = "bid:\"" + uc.getBid() + "\"";

					qr = find.find(query);
					if (qr.getResponse() != null && qr.getResponse().get("response") != null) {
						response = (SolrDocumentList) qr.getResponse().get("response");
						if (response.getNumFound() > 0) {
							objectIdentifier = (String) response.get(0).get(ItemTeca.ID);
						}
					}
				} catch (NumberFormatException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				} catch (SolrServerException e) {
					log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
					throw new JobExecutionException(e.getMessage(), e, false);
				} finally {
					if (find != null) {
						find.close();
					}
				}
				if (objectIdentifier == null) {
					objectIdentifier = UUID.randomUUID().toString();
				}
				fMagIndex = new File(folderIndex.getAbsolutePath() + File.separator
						+ f.getName().replace(" ", "_").replace(".xls", "_mag.xml"));
				publisher(objectIdentifier, fMagIndex.getAbsolutePath(), uc, new MagXsd(), admd);
				fSolr = new File(fMagIndex.getAbsolutePath() + ".solr");
				admd.write(fSolr);
				admd.publish(Configuration.getValue("solr.batchPost"), fSolr);

				log.info("[" + QuartzTools.getName(context) + "] Fine pubblicazione");
				printFile(f, null, "File elaborato con successo");
			}
			msg = "Processo: +" + context.getJobDetail().getKey().getGroup() + " => "
					+ context.getJobDetail().getKey().getName() + " => " + context.getTrigger().getKey().getGroup()
					+ " => " + context.getTrigger().getKey().getName() + " terminato regolarmente";
		} catch (SchedulerException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
			// } catch (XsdException e) {
			// log.error("["+QuartzTools.getName(context)+"]
			// "+e.getMessage(),e);
			// throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (IOException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (BiffException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (IndexOutOfBoundsException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (JFileException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			if (wb != null) {
				wb.close();
			}
		}
		return msg;
	}

	private void printFile(File fSolr, Exception err, String msgOut) throws IOException {
		File fOut = null;
		BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			fOut = new File(fSolr.getAbsolutePath() + (err == null ? ".elabOK" : ".elabKO"));
			fw = new FileWriter(fOut);
			bw = new BufferedWriter(fw);

			if (msgOut != null) {
				bw.write(msgOut);
			}
			if (err != null) {
				bw.write(err.getMessage() + "\n");
				err.printStackTrace(new PrintWriter(bw));
			}
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (bw != null) {
					bw.flush();
					bw.close();
				}
				if (fw != null) {
					fw.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}

	public static Workbook openFileXls(String fileXls) throws BiffException, IndexOutOfBoundsException, IOException {
		File f = null;
		Workbook wb = null;
		WorkbookSettings wbs = null;

		try {
			f = new File(fileXls);
			if (f.exists()) {
				wbs = new WorkbookSettings();
				wbs.setEncoding("ISO-8859-1");
				// wbs.setEncoding("UTF-8");
				wb = Workbook.getWorkbook(f);
			} else {
				System.out.println("Il file [" + f.getAbsolutePath() + "] non esiste");
			}
		} catch (BiffException e) {
			throw e;
		} catch (IndexOutOfBoundsException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		return wb;
	}

	protected void publisher(String objectIdentifier, String filename, UC md, MagXsd mdXsd, IndexDocumentTeca admd)
			throws SolrException {

		try {
			params = new Params();
			params.getParams().clear();

			params.add(ItemTeca.ID, objectIdentifier);
			// params.add(ItemTeca._ROOT_, objectIdentifier);
			params.add(ItemTeca.TIPOOGGETTO, ItemTeca.TIPOOGGETTO_DOCUMENTO);
			params.add(ItemTeca.TIPOLOGIAFILE, ItemTeca.TIPOLOGIAFILE_UC);
			params.add(ItemTeca.ORIGINALFILENAME, filename);

			read(ItemTeca.BID, md.getBid());
			read(ItemTeca.SOGGETTOCONSERVATORE, md.getSoggettoConservatore());
			read(ItemTeca.SOGGETTOCONSERVATOREKEY, md.getSoggettoConservatoreKey());
			read(ItemTeca.FONDO, md.getFondo());
			read(ItemTeca.FONDOKEY, md.getFondoKey());
			read(ItemTeca.SUBFONDO, md.getSubFondo());
			read(ItemTeca.SUBFONDOKEY, md.getSubFondoKey());
			read(ItemTeca.COLLOCAZIONE, md.getCollocazione());
			read(ItemTeca._ROOT_, md.getRoot());
			read(ItemTeca._ROOTDESC_, md.getRootDesc());
			read(ItemTeca.TIPOLOGIAMATERIALE, md.getTipologiaMateriale());
			read(ItemTeca.TITOLO, md.getTitolo());
			read(ItemTeca.LINGUA, md.getLingua());
			read(ItemTeca.DATACRONICA, md.getDataCronica());
			read(ItemTeca.DATATOPICA, md.getDataTopica());
			read(ItemTeca.SUPPORTO, md.getSupporto());
			read(ItemTeca.TECNICHE, md.getTecniche());
			read(ItemTeca.DIMENSIONE, md.getDimensione());
			read(ItemTeca.SCALA, md.getScala());
			read(ItemTeca.STATOCONSERVAZIONE, md.getStatoConservazione());
			read(ItemTeca.AUTORE, md.getAutori());
			read(ItemTeca.DATIFRUIZIONE, md.getDatiFruizione());
			read(ItemTeca.COMPILATORE, md.getCompilatore());
			read(ItemTeca.DATACOMPILAZIONE, md.getDataCompilazione());
			read(ItemTeca.NOTE, md.getNote());

			params.add(ItemTeca.XML, mdXsd.write(md.getMagIndex(), new MagNamespacePrefix(), null, null, null));

			admd.add(params.getParams(), new ItemTeca());
		} catch (SolrException e) {
			throw e;
		} catch (XsdException e) {
			throw new SolrException(e.getMessage(), e);
		}
	}
}
