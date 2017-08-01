/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file;

import it.sbn.iccu.metaag1.Bib;
import it.sbn.iccu.metaag1.BibliographicLevel;
import it.sbn.iccu.metaag1.Gen;
import it.sbn.iccu.metaag1.Img;
import it.sbn.iccu.metaag1.Bib.Holdings;
import it.sbn.iccu.metaag1.Bib.Holdings.Shelfmark;
import it.sbn.iccu.metaag1.Img.Altimg;
import it.sbn.iccu.metaag1.Link;
import it.sbn.iccu.metaag1.Metadigit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.interfacException.exception.PubblicaException;
import mx.randalf.mag.MagNamespacePrefix;
import mx.randalf.mag.MagXsd;
import mx.randalf.quartz.QuartzTools;
import mx.randalf.schedaF.CsmRoot;
import mx.randalf.schedaF.CsmRootXsd;
import mx.randalf.schedaF.Scheda;
import mx.randalf.schedaF.Scheda.AU.AUF;
import mx.randalf.schedaF.Scheda.CM.CMP.CMPN;
import mx.randalf.schedaF.Scheda.CM.FUR;
import mx.randalf.schedaF.Scheda.CM.RSR;
import mx.randalf.schedaF.Scheda.DO.FTA;
import mx.randalf.schedaF.Scheda.SG.SGL;
import mx.randalf.schedaF.Scheda.SG.SGT.SGTI;
import mx.randalf.schedaF.SchedaXsd;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.randalf.tools.Utils;
import mx.randalf.tools.exception.UtilException;
import mx.randalf.xsd.exception.XsdException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.exception.JFileException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.xlsx.ToolsXsl;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.im4java.process.ProcessStarter;
import org.purl.dc.elements._1.SimpleLiteral;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

/**
 * @author massi
 *
 */
public class JSchedaF extends JFile<Scheda, SchedaXsd> {

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(JSchedaF.class);

	public static String TYPE = "schedaF";

	private Hashtable<String, String> nomenclature = null;
	private File folderIndex = null;
	private File folderOri = null;
	private JobExecutionContext context = null;
	private Boolean genMagComp = null;
	private Boolean genMagPub = null;

	/**
	 * 
	 */
	public JSchedaF() {
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String msg = null;
		File f = null;
		File fSolr = null;
		IndexDocumentTeca admd = null;
		CsmRootXsd magXsd = null;
		CsmRoot md = null;
		String objectIdentifier = null;
		FindDocument find = null;
		QueryResponse qr = null;
		String query = "";
		SolrDocumentList response = null;
		Scheda scheda = null;

		try {
			this.context = context;
			ProcessStarter.setGlobalSearchPath(Configuration.getValue("calcImg.path"));
			f = (File) context.getJobDetail().getJobDataMap().get(FILE);
			folderIndex = (File) context.getJobDetail().getJobDataMap().get(FOLDERINDEX);
			folderOri = (File) context.getJobDetail().getJobDataMap().get(FOLDERORI);

			if (context.getJobDetail().getJobDataMap().get(GENMAGCOMP) != null)
				genMagComp = (Boolean) context.getJobDetail().getJobDataMap().get(GENMAGCOMP);

			if (context.getJobDetail().getJobDataMap().get(GENMAGPUB) != null)
				genMagPub = (Boolean) context.getJobDetail().getJobDataMap().get(GENMAGPUB);

			log.info("[" + QuartzTools.getName(context) + "] Inizio la pubblicazione del file [" + f.getAbsolutePath()
					+ "]");
			log.debug("[" + QuartzTools.getName(context) + "] folderIndex: " + folderIndex.getAbsolutePath());
			log.debug("[" + QuartzTools.getName(context) + "] folderOri: " + folderOri.getAbsolutePath());

			readNomenclature(f);
			magXsd = new CsmRootXsd();
			log.debug("[" + QuartzTools.getName(context) + "] Apro il file : " + f.getAbsolutePath());
			md = magXsd.read(f);
			admd = new IndexDocumentTeca();
			for (int x = 0; x < md.getSchede().getScheda().size(); x++) {
				log.info("[" + QuartzTools.getName(context) + "] Analisi scheda " + (x + 1) + "/"
						+ md.getSchede().getScheda().size());
				scheda = md.getSchede().getScheda().get(x);
				try {
					find = new FindDocument(Configuration.getValue("solr.URL"),
							Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
							Configuration.getValue("solr.collection"),
							Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
							Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
					query = "bid:\"" + scheda.getCD().getNCT().getNCTR().getValue()
							+ scheda.getCD().getNCT().getNCTN().getValue() + "\"";

					// if (md.getBib().getPiece() != null){
					// if (md.getBib().getPiece().getYear() != null ||
					// md.getBib().getPiece().getPartNumber() != null){
					// query += " +piecegr:\""+
					// (md.getBib().getPiece().getYear() !=
					// null?md.getBib().getPiece().getYear():md.getBib().getPiece().getPartNumber().toString())+
					// "\"";
					// }
					// if (md.getBib().getPiece().getIssue() != null ||
					// md.getBib().getPiece().getPartName() != null){
					// query += " +piecedt:\""+
					// (md.getBib().getPiece().getIssue() !=
					// null?md.getBib().getPiece().getIssue():md.getBib().getPiece().getPartName())+
					// "\"";
					// }
					// }

					log.debug("[" + QuartzTools.getName(context) + "] Solr Find " + query);
					qr = find.find(query);
					objectIdentifier = null;
					if (qr.getResponse() != null && qr.getResponse().get("response") != null) {
						response = (SolrDocumentList) qr.getResponse().get("response");
						if (response.getNumFound() > 0) {
							objectIdentifier = (String) response.get(0).get(ItemTeca.ID);
							log.debug("[" + QuartzTools.getName(context) + "] Trovato ID " + objectIdentifier);
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
				try {
					// fMagIndex = new File(folderIndex.getAbsolutePath()+
					// File.separator+
					// f.getName().replace(" ", "_").replace(".xls",
					// "_mag.xml"));
					fSolr = new File(folderIndex.getAbsolutePath() + File.separator
							+ scheda.getCD().getNCT().getNCTR().getValue()
							+ scheda.getCD().getNCT().getNCTN().getValue() + "_mag.xml.solr");
					// if (!fSolr.exists()){
					log.debug("[" + QuartzTools.getName(context) + "] Inizio publisher ID: " + objectIdentifier);
					publisher(objectIdentifier,
							f.getParentFile().getAbsolutePath() + File.separator
									+ scheda.getCD().getNCT().getNCTR().getValue()
									+ scheda.getCD().getNCT().getNCTN().getValue() + "_mag.xml",
							scheda, new SchedaXsd(), admd);
					log.debug("[" + QuartzTools.getName(context) + "] Fine publisher ID: " + objectIdentifier);
					if (genMagPub) {
						log.debug("[" + QuartzTools.getName(context) + "] Inizio Write file Solr ID: "
								+ objectIdentifier + " file: " + fSolr.getAbsolutePath());
						admd.write(fSolr);
						log.debug("[" + QuartzTools.getName(context) + "] Fine Write file Solr ID: " + objectIdentifier
								+ " file: " + fSolr.getAbsolutePath());
						log.debug("[" + QuartzTools.getName(context) + "] Inizio publish Solf ID: " + objectIdentifier);
						admd.publish(Configuration.getValue("solr.batchPost"), fSolr);
						log.debug("[" + QuartzTools.getName(context) + "] Fine publish Solf ID: " + objectIdentifier);
						// }
						printFile(f, null, "File elaborato con successo");
					}
				} catch (FileNotFoundException e) {
					try {
						if (f != null) {
							printFile(f, e, null);
						}
					} catch (IOException e1) {
						log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
					}
					System.out.println(e.getMessage());
				}
			}
			log.info("[" + QuartzTools.getName(context) + "] Fine pubblicazione");
			msg = "[" + QuartzTools.getName(context) + "] terminato regolarmente";
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
		} catch (XsdException e) {
			try {
				if (f != null) {
					printFile(f, e, null);
				}
			} catch (IOException e1) {
				log.error("[" + QuartzTools.getName(context) + "] " + e1.getMessage(), e1);
			}
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
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

	private void readNomenclature(File fOri) throws BiffException, IndexOutOfBoundsException, IOException {
		File f = null;
		Workbook wb = null;
		Sheet finestra = null;
		Cell[] riga = null;
		String key = null;
		String value = null;

		try {
			nomenclature = new Hashtable<String, String>();
			f = new File(fOri.getParentFile().getAbsolutePath() + File.separator
					+ fOri.getName().replace(".xml", "_Nomenclatura.xls"));
			if (f.exists()) {
				// Apro il file xlsx
				log.debug("[" + QuartzTools.getName(context) + "] Apro il file Nomenclatura: " + f.getAbsolutePath());
				wb = JUC.openFileXls(f.getAbsolutePath());

				// Leggo le infomrazioni della 1 finestra
				finestra = wb.getSheet(0);
				for (int x = 1; x < finestra.getRows(); x++) {
					log.debug("[" + QuartzTools.getName(context) + "] Leggo la riga  " + x + "/"
							+ (finestra.getRows() - 1));
					riga = finestra.getRow(x);
					key = ToolsXsl.analizza(riga[0]);
					value = ToolsXsl.analizza(riga[1]);
					if (key != null && !key.trim().equals("") && value != null && !value.trim().equals("")) {
						key = key.replace("‐", "-").replace("capoluogo", "CAPOLUOGO");
						key = key.replace("_curiosità_", "_curiosit_");
						key = key.replace("_bnatura_", "_bpnatura_");
						key = key.replace("_risorse_s81_08", "_risorse_s81_8");
						key = key.replace("_risorse_s82_08", "_risorse_s82_8");
						key = key.replace("_risorse_s83_08", "_risorse_s83_8");
						key = key.replace("_risorse_s87_08", "_risorse_s87_8");
						if (key.indexOf("cl_II") > -1) {
							key = key.replace("_r", "");
						}
						// System.out.println("."+key+". - "+value);
						nomenclature.put(key, value);
					}
				}

			}
		} catch (BiffException e) {
			throw e;
		} catch (IndexOutOfBoundsException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			if (wb != null) {
				log.debug("[" + QuartzTools.getName(context) + "] Chiudo il file Nomenclatura: " + f.getAbsolutePath());
				wb.close();
			}
		}
	}

	protected void publisher(String objectIdentifier, String filename, Scheda scheda, SchedaXsd mdXsd,
			IndexDocumentTeca admd) throws SolrException, FileNotFoundException {
		MagXsd magXsd = null;
		File fMag = null;
		File fMagIndex = null;
		Vector<String> titoli = null;
		Vector<String> autori = null;
		Vector<String> soggetti = null;
		Vector<String> note = null;
		String collocazione = null;
		// File fOri = null;

		try {
			fMag = new File(filename);
			fMagIndex = new File(folderIndex.getAbsolutePath() + File.separator + fMag.getName());
			magXsd = new MagXsd();
			params = new Params();
			params.getParams().clear();

			params.add(ItemTeca.ID, objectIdentifier);
			// params.add(ItemTeca._ROOT_, objectIdentifier);
			params.add(ItemTeca.TIPOOGGETTO, ItemTeca.TIPOOGGETTO_SCHEDAF);
			params.add(ItemTeca.TIPOLOGIAFILE, ItemTeca.TIPOLOGIAFILE_SCHEDAF);
			params.add(ItemTeca.ORIGINALFILENAME, fMagIndex.getAbsolutePath());

			read(ItemTeca.BID,
					scheda.getCD().getNCT().getNCTR().getValue() + scheda.getCD().getNCT().getNCTN().getValue());

			read(ItemTeca.ENTESCHEDATORE, scheda.getCD().getESC().getValue());
			read(ItemTeca.ENTECOMPETENTE, scheda.getCD().getECP().getValue());
			readSGTI(ItemTeca.SOGGETTO, scheda.getSG().getSGT().getSGTI());

			if (scheda.getSG().getSGT() != null && scheda.getSG().getSGT().getSGTI() != null
					&& scheda.getSG().getSGT().getSGTI().size() > 0) {
				for (int x = 0; x < scheda.getSG().getSGT().getSGTI().size(); x++) {
					if (scheda.getSG().getSGT().getSGTI().get(x).getValue() != null) {
						if (soggetti == null) {
							soggetti = new Vector<String>();
						}
						soggetti.add(scheda.getSG().getSGT().getSGTI().get(x).getValue());
					}
				}
			}

			readSGL(ItemTeca.TITOLO, scheda.getSG().getSGL());

			if (scheda.getSG().getSGL() != null && scheda.getSG().getSGL().size() > 0) {
				for (int x = 0; x < scheda.getSG().getSGL().size(); x++) {
					if (scheda.getSG().getSGL().get(x).getSGLT() != null
							&& scheda.getSG().getSGL().get(x).getSGLT().getValue() != null) {
						if (titoli == null) {
							titoli = new Vector<String>();
						}
						titoli.add(scheda.getSG().getSGL().get(x).getSGLT().getValue());
					}
					if (scheda.getSG().getSGL().get(x).getSGLA() != null
							&& scheda.getSG().getSGL().get(x).getSGLA().getValue() != null) {
						if (titoli == null) {
							titoli = new Vector<String>();
						}
						titoli.add(scheda.getSG().getSGL().get(x).getSGLA().getValue());
					}
				}
			}

			read(ItemTeca.STATO, scheda.getLC().getPVC().getPVCS().getValue());
			read(ItemTeca.REGIONE, scheda.getLC().getPVC().getPVCR().getValue());
			read(ItemTeca.PROVINCIA, scheda.getLC().getPVC().getPVCP().getValue());
			read(ItemTeca.COMUNE, scheda.getLC().getPVC().getPVCC().getValue());

			if (scheda.getLC().getLDC().getLDCN().getValue().equals("non identificabile")) {
				read(ItemTeca.SOGGETTOCONSERVATOREKEY, "ND");
			} else {
				String testo = null;
				String[] st = null;
				testo = fMag.getAbsolutePath().replace(folderOri.getAbsolutePath() + File.separator, "");
				st = testo.split(File.separator);
				read(ItemTeca.SOGGETTOCONSERVATOREKEY, st[0]);
			}
			String soggettoConservatore = null;
			soggettoConservatore = scheda.getLC().getLDC().getLDCN().getValue();
			if (soggettoConservatore.equalsIgnoreCase("Biblioteca Pubblica Arcivescovile \"Annibale De Leo\"")
					|| soggettoConservatore.equalsIgnoreCase("Biblioteca Arcivescovile A. De Leo Brindisi")) {
				soggettoConservatore = "Biblioteca pubblica arcivescovile Annibale De Leo";
			} else if (soggettoConservatore.equalsIgnoreCase("Biblioteca comunale \"Filippo De Miccolis Angelini\"")
					|| soggettoConservatore.equalsIgnoreCase("Biblioteca Comunale \"Filippo De Miccolis Angelini\"")) {
				soggettoConservatore = "Biblioteca comunale \"Filippo De Miccolis Angelini\"";
			} else if (soggettoConservatore.equalsIgnoreCase("Biblioteca Provinciale")) {
				soggettoConservatore = "Biblioteca Provinciale di Foggia La Magna Capitana";
			}
			read(ItemTeca.SOGGETTOCONSERVATORE, soggettoConservatore);

			read(ItemTeca.DENOMINAZIONE, scheda.getLC().getLDC().getLDCN().getValue());
			read(ItemTeca.DENOMINAZIONEINDIRIZZO, scheda.getLC().getLDC().getLDCU().getValue());
			read(ItemTeca.DENOMINAZIONERACCOLTA, scheda.getLC().getLDC().getLDCM().getValue());

			String fondo = null;
			fondo = scheda.getUB().getUBF().getUBFP().getValue();
			if (fondo.equalsIgnoreCase("Archivio Fotografico Pasquale Di Mizio")
					|| fondo.equalsIgnoreCase("Archivio Fotografico pasquale Di Mizio")) {
				fondo = "Archivio Fotografico Pasquale Di Mizio";
			} else if (fondo.equalsIgnoreCase("fondo Rosario Labadessa")) {
				fondo = "Fondo Rosario Labadessa";
			}
			read(ItemTeca.FONDO, fondo);
			if (scheda.getUB().getUBF().getUBFQ() != null) {
				read(ItemTeca.FONDOSPECIFICHE, scheda.getUB().getUBF().getUBFQ().getValue());
			}
			collocazione = scheda.getUB().getUBF().getUBFC().getValue();
			read(ItemTeca.COLLOCAZIONE, scheda.getUB().getUBF().getUBFC().getValue());
			read(ItemTeca.SECOLOINIZIALE, scheda.getDT().getDTZ().getDTZG().getValue());
			if (scheda.getDT().getDTZ().getDTZS() != null) {
				read(ItemTeca.SECOLOFINALE, scheda.getDT().getDTZ().getDTZS().getValue());
			}
			if (scheda.getDT().getDTS() != null) {
				if (scheda.getDT().getDTS().getDTSI() != null) {
					read(ItemTeca.DATA, scheda.getDT().getDTS().getDTSI().getValue());
				}
				if (scheda.getDT().getDTS().getDTSF() != null) {
					read(ItemTeca.DATA, scheda.getDT().getDTS().getDTSF().getValue());
				}
			}
			if (scheda.getAU() != null && scheda.getAU().getAUF() != null) {
				readAUF(ItemTeca.AUTORE, scheda.getAU().getAUF());
			}

			if (scheda.getAU() != null && scheda.getAU().getAUF() != null && scheda.getAU().getAUF().size() > 0) {
				for (int x = 0; x < scheda.getAU().getAUF().size(); x++) {
					if (scheda.getAU().getAUF().get(x).getAUFN() != null
							&& scheda.getAU().getAUF().get(x).getAUFN().getValue() != null) {
						if (autori == null) {
							autori = new Vector<String>();
						}
						autori.add(scheda.getAU().getAUF().get(x).getAUFN().getValue());
					}
					if (scheda.getAU().getAUF().get(x).getAUFB() != null
							&& scheda.getAU().getAUF().get(x).getAUFB().getValue() != null) {
						if (autori == null) {
							autori = new Vector<String>();
						}
						autori.add(scheda.getAU().getAUF().get(x).getAUFB().getValue());
					}
				}
			}

			readCMPN(ItemTeca.COMPILATORE, scheda.getCM().getCMP().getCMPN());
			read(ItemTeca.DATACOMPILAZIONE, scheda.getCM().getCMP().getCMPD().getValue());
			readRSR(ItemTeca.REFERENTESCIENTIFICO, scheda.getCM().getRSR());
			readFUR(ItemTeca.FUNZIONARIORESPONSABILE, scheda.getCM().getFUR());

			if (scheda.getLR() != null) {
				if (scheda.getLR().getLRA() != null) {
					if (note == null) {
						note = new Vector<String>();
					}
					note.add(scheda.getLR().getLRA().getValue());
				}
				if (scheda.getLR().getLRC() != null && scheda.getLR().getLRC().getLRCS() != null) {
					if (note == null) {
						note = new Vector<String>();
					}
					note.add(scheda.getLR().getLRC().getLRCS().getValue());
				}
				if (scheda.getLR().getLRC() != null && scheda.getLR().getLRC().getLRCC() != null) {
					if (note == null) {
						note = new Vector<String>();
					}
					note.add(scheda.getLR().getLRC().getLRCC().getValue());
				}
			}

			if (genMagComp) {
				genMag(fMag, fMagIndex,
						scheda.getCD().getNCT().getNCTR().getValue() + scheda.getCD().getNCT().getNCTN().getValue(),
						scheda.getDO().getFTA(), folderIndex, genMagComp, genMagPub, titoli, autori, soggetti, note,
						collocazione);
			}

			if (genMagPub) {
				log.debug("[" + QuartzTools.getName(context) + "] Scrittora file XmlSchedaF");
				params.add(ItemTeca.XMLSCHEDAF, mdXsd.write(scheda, new MagNamespacePrefix(), null, null, null));

				log.debug("[" + QuartzTools.getName(context) + "] Scrittora file XmlMag");
				params.add(ItemTeca.XML, magXsd.write(
						genMag(fMag, fMagIndex,
								scheda.getCD().getNCT().getNCTR().getValue()
										+ scheda.getCD().getNCT().getNCTN().getValue(),
								scheda.getDO().getFTA(), folderIndex, genMagComp, genMagPub, titoli, autori, soggetti,
								note, collocazione),
						new MagNamespacePrefix(), null, null, null));
			}
			admd.clear();
			admd.add(params.getParams(), new ItemTeca());
		} catch (SolrException e) {
			throw e;
		} catch (XsdException e) {
			throw new SolrException(e.getMessage(), e);
		} catch (JFileException e) {
			throw new SolrException(e.getMessage(), e);
		} catch (FileNotFoundException e) {
			throw e;
		}
	}

	private Metadigit genMag(File fMag, File fMagIndex, String bid, List<FTA> fta, File folderIndex, boolean genMagComp,
			boolean genMagPub, Vector<String> titoli, Vector<String> autori, Vector<String> soggetti,
			Vector<String> note, String collocazione) throws JFileException, FileNotFoundException {
		// File f = null;
		// File fMag = null;
		// File fMagIndex = null;
		Gen gen = null;
		Bib bib = null;
		SimpleLiteral identifier = null;
		SimpleLiteral title = null;
		// SimpleLiteral language = null;
		SimpleLiteral autore = null;
		SimpleLiteral subject = null;
		SimpleLiteral description = null;
		Holdings holdings = null;
		Shelfmark shelfmark = null;
		MagXsd magXsd = null;
		Metadigit mag = null;
		Metadigit magIndex = null;
		Img img = null;
		// String[] st = null;
		String folderImg = null;
		// File fImg = null;
		Link file = null;
		Altimg altImg = null;
		String nomeFileImg = null;
		String fileOri = null;
		String fileDes = null;

		try {
			// if (nomenclature.get(nomeFileImg+".jpg")!= null){
			folderImg = "./";
			mag = new Metadigit();
			magIndex = new Metadigit();

			gen = new Gen();
			gen.setAccessRights(new BigInteger("1"));
			gen.setAgency(
					"SEGRETARIATO REGIONALE DEL MINISTERO DEI BENI E DELLE ATTIVITA' CULTURALI E DEL TURISMO PER LA PUGLIA");
			gen.setCollection(
					"http://www.beniculturali.it/mibac/export/MiBAC/sito-MiBAC/Luogo/MibacUnif/Enti/visualizza_asset.html_2000168143.html");
			gen.setCompleteness(new BigInteger("0"));
			gen.setCreation(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()));
			gen.setLastUpdate(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()));
			gen.setStprog(
					"http://www.beniculturali.it/mibac/export/MiBAC/sito-MiBAC/Luogo/MibacUnif/Enti/visualizza_asset.html_2000168143.html");
			mag.setGen(gen);
			magIndex.setGen(gen);

			bib = new Bib();
			bib.setLevel(BibliographicLevel.M);
			identifier = new SimpleLiteral();
			identifier.getContent().add(bid);
			bib.getIdentifier().add(identifier);

			if (titoli != null && titoli.size() > 0) {
				for (int x = 0; x < titoli.size(); x++) {
					title = new SimpleLiteral();
					title.getContent().add(titoli.get(x));
					bib.getTitle().add(title);
				}
			}

			if (autori != null && autori.size() > 0) {
				for (int x = 0; x < autori.size(); x++) {

					autore = new SimpleLiteral();
					autore.getContent().add(autori.get(x));
					bib.getCreator().add(autore);
				}
			}
			// if (lingua != null && !lingua.trim().equals("")){
			// language = new SimpleLiteral();
			// language.getContent().add(lingua);
			// bib.getLanguage().add(language);
			// }

			if (soggetti != null && soggetti.size() > 0) {
				for (int x = 0; x < soggetti.size(); x++) {
					subject = new SimpleLiteral();
					subject.getContent().add(soggetti.get(x));
					bib.getSubject().add(subject);
				}
			}

			if (note != null && note.size() > 0) {
				for (int x = 0; x < note.size(); x++) {
					description = new SimpleLiteral();
					description.getContent().add(note.get(x));
					bib.getDescription().add(description);
				}
			}

			if (collocazione != null && !collocazione.trim().equals("")) {

				holdings = new Holdings();
				shelfmark = new Shelfmark();
				shelfmark.setContent(collocazione);
				holdings.getShelfmark().add(shelfmark);
				bib.getHoldings().add(holdings);
			}

			mag.setBib(bib);
			magIndex.setBib(bib);

			magXsd = new MagXsd();

			for (int x = 0; x < fta.size(); x++) {
				log.debug("[" + QuartzTools.getName(context) + "] File XmlMag Immagini " + (x + 1) + "/" + fta.size());
				nomeFileImg = fta.get(x).getFTAN().getValue();
				// nomeFileImg = nomeFileImg.replace("_curiosit_",
				// "_curiosità_");
				nomeFileImg = nomeFileImg.replace("_ILFO4_", "_ILFORD4_");
				nomeFileImg = nomeFileImg.replace("_ILFOR4_", "_ILFORD4_");
				nomeFileImg = nomeFileImg.replace("ASBr-GC-b47-", "ASBr-GC-cl_II-tit1-b47-");

				if (nomeFileImg.equals("Foglio Trasparente, busta")) {
					nomeFileImg = "FPDM_FT_b2_69";
				}

				String nom = nomenclature.get(nomeFileImg + ".jpg");
				if (nom == null && nomeFileImg.endsWith("_r")) {
					nom = nomenclature.get(nomeFileImg.substring(0, nomeFileImg.length() - 2) + ".jpg");
					if (nom != null) {
						nom += " (Recto)";
					}
				}
				if (nom == null && nomeFileImg.endsWith("_v")) {
					nom = nomenclature.get(nomeFileImg.substring(0, nomeFileImg.length() - 2) + ".jpg");
					if (nom != null) {
						nom += " (Verso)";
					}
				}
				if (nom == null) {
					String msgErr = "";
					msgErr = "Nomenclatura mancante [" + nomeFileImg + ".jpg] ";
					if (nomenclature != null) {
						Enumeration<String> keys = nomenclature.keys();
						if (keys != null) {
							msgErr += "in [";
							while (keys.hasMoreElements()) {
								msgErr += keys.nextElement() + ", ";
							}
							msgErr += "]";
						}
					}
					throw new JFileException(msgErr);
				}

				if (genMagPub) {
					img = new Img();
					img.setNomenclature(nom);
					img.setSequenceNumber(new BigInteger((x + 1) + ""));
					img.getUsage().add("3");
					file = new Link();
					file.setHref(folderImg + "150/" + nomeFileImg + ".jpg");
					img.setFile(file);
					fileOri = fMag.getParentFile().getAbsolutePath() + File.separator + folderImg.replace("./", "")
							+ "150/" + nomeFileImg + ".jpg";
					fileDes = folderIndex.getAbsolutePath() + File.separator + folderImg.replace("./", "") + "150/"
							+ nomeFileImg + ".jpg";
					if (!new File(fileOri).exists()) {
						throw new JFileException("Il file [" + fileOri + "] non esiste ");
					}
					log.debug("[" + QuartzTools.getName(context) + "] File XmlMag Immagini " + (x + 1) + "/"
							+ fta.size() + " copio il file " + fileOri + " in " + fileDes);
					if (!Utils.copyFileValidate(fileOri, fileDes, false)) {
						throw new JFileException(
								"Problemi nella copia del file [" + fileOri + "] in [" + fileDes + "]");
					}
					log.debug("[" + QuartzTools.getName(context) + "] File XmlMag Immagini " + (x + 1) + "/"
							+ fta.size() + " calcolo Img del file " + fileDes);

					magXsd.calcImg(img, folderIndex.getAbsolutePath());
					log.debug("[" + QuartzTools.getName(context) + "] File XmlMag Immagini " + (x + 1) + "/"
							+ fta.size() + " calcolo Img del file " + fileDes + " fatto");
					magIndex.getImg().add(img);
				}

				if (genMagComp) {
					img = new Img();
					img.setNomenclature(nom);
					img.setSequenceNumber(new BigInteger((x + 1) + ""));
					img.getUsage().add("1");
					file = new Link();
					file.setHref(folderImg + "TIF/" + nomeFileImg + ".tif");
					img.setFile(file);

					altImg = new Altimg();
					altImg.getUsage().add("2");
					file = new Link();
					file.setHref(folderImg + "300/" + nomeFileImg + ".jpg");
					altImg.setFile(file);
					img.getAltimg().add(altImg);

					altImg = new Altimg();
					altImg.getUsage().add("3");
					file = new Link();
					file.setHref(folderImg + "150/" + nomeFileImg + ".jpg");
					altImg.setFile(file);
					img.getAltimg().add(altImg);

					magXsd.calcImg(img, fMag.getParentFile().getAbsolutePath());
					mag.getImg().add(img);
				}
			}

			if (genMagComp) {
				magXsd.write(mag, fMag);
			}
			if (genMagPub) {
				log.debug(
						"[" + QuartzTools.getName(context) + "] File XmlMag Write MAG " + fMagIndex.getAbsolutePath());
				magXsd.write(magIndex, fMagIndex);
				log.debug("[" + QuartzTools.getName(context) + "] File XmlMag Write MAG " + fMagIndex.getAbsolutePath()
						+ " fatto");
			}
			// } else {
			// throw new FileNotFoundException("Non trovata la nomenclatura per
			// il file "+nomeFileImg);
			// }
		} catch (IndexOutOfBoundsException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (DatatypeConfigurationException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (XsdException e) {
			throw new JFileException(e.getMessage(), e);
			// } catch (FileNotFoundException e) {
			// throw e;
		} catch (UtilException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (PubblicaException e) {
			throw new JFileException(e.getMessage(), e);
		}
		return magIndex;
	}

	private void readSGTI(String key, List<SGTI> values) {
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				params.add(key, ((SGTI) values.get(x)).getValue());
			}
		}
	}

	private void readSGL(String key, List<SGL> values) {
		SGL sgl = null;
		String testo = null;
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				sgl = ((SGL) values.get(x));
				testo = "";
				if (sgl.getSGLT() != null && sgl.getSGLT().getValue() != null) {
					testo = sgl.getSGLT().getValue();
				}
				if (sgl.getSGLA() != null && sgl.getSGLA().getValue() != null) {
					testo += (testo.equals("") ? "" : " ; ") + sgl.getSGLA().getValue();
				}
				if (sgl.getSGLL() != null && sgl.getSGLL().getValue() != null) {
					testo += (testo.equals("") ? "" : " ; ") + sgl.getSGLL().getValue();
				}
				if (sgl.getSGLS() != null && sgl.getSGLS().getValue() != null) {
					testo += (testo.equals("") ? "" : " ; ") + sgl.getSGLS().getValue();
				}
				params.add(key, testo);
			}
		}
	}

	private void readAUF(String key, List<AUF> values) {
		AUF auf = null;
		String testo = null;
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				auf = ((AUF) values.get(x));
				testo = "";
				if (auf.getAUFN() != null && auf.getAUFN().getValue() != null) {
					testo = auf.getAUFN().getValue();
				}
				if (auf.getAUFA() != null && auf.getAUFA().getValue() != null) {
					testo += (testo.equals("") ? "(" : " (") + auf.getAUFA().getValue() + ")";
				}
				params.add(key, testo);
			}
		}
	}

	private void readCMPN(String key, List<CMPN> values) {
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				params.add(key, ((CMPN) values.get(x)).getValue());
			}
		}
	}

	private void readRSR(String key, List<RSR> values) {
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				params.add(key, ((RSR) values.get(x)).getValue());
			}
		}
	}

	private void readFUR(String key, List<FUR> values) {
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				params.add(key, ((FUR) values.get(x)).getValue());
			}
		}
	}
}
