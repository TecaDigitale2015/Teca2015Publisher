/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file;

import it.sbn.iccu.metaag1.Bib.Holdings;
import it.sbn.iccu.metaag1.Bib.Piece;
import it.sbn.iccu.metaag1.BibliographicLevel;
import it.sbn.iccu.metaag1.Metadigit;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.mag.MagNamespacePrefix;
import mx.randalf.mag.MagXsd;
import mx.randalf.quartz.QuartzTools;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.randalf.xsd.exception.XsdException;
import mx.teca2015.tecaPublished.standAlone.quartz.folder.Folder;
import mx.teca2015.tecaPublished.standAlone.quartz.folder.JFolder;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.purl.dc.elements._1.SimpleLiteral;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

/**
 * @author massi
 *
 */
public class JMag extends JFile<Metadigit, MagXsd> {

	/**
	 * Variabile utilizzata per loggare l'applicazione
	 */
	private static Logger log = Logger.getLogger(JMag.class);

	public static String TYPE = "mag";

	private Folder folder = null;

	/**
	 * 
	 */
	public JMag() {
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String msg = null;
		File f = null;
		File fSolr = null;
		IndexDocumentTeca admd = null;
		MagXsd magXsd = null;
		Metadigit md = null;
		String objectIdentifier = null;
		FindDocument find = null;
		QueryResponse qr = null;
		String query = "";
		SolrDocumentList response = null;

		try {
			f = (File) context.getJobDetail().getJobDataMap().get(FILE);
			folder = (Folder) context.getJobDetail().getJobDataMap().get(JFolder.FOLDER);
			log.info("[" + QuartzTools.getName(context) + "] Inizio la pubblicazione del file [" + f.getAbsolutePath()
					+ "]");

			magXsd = new MagXsd();
			md = magXsd.read(f);
			admd = new IndexDocumentTeca();

			try {
				find = new FindDocument(Configuration.getValue("solr.URL"),
						Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
						Configuration.getValue("solr.collection"),
						Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
						Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
				query = "+bid:\"" + md.getBib().getIdentifier().get(0).getContent().get(0).replace("\\", "") + "\"";

				if (md.getBib().getPiece() != null) {
					if (md.getBib().getPiece().getYear() != null || md.getBib().getPiece().getPartNumber() != null) {
						query += " +piecegr:\""
								+ (md.getBib().getPiece().getYear() != null ? md.getBib().getPiece().getYear()
										: md.getBib().getPiece().getPartNumber().toString())
								+ "\"";
					}
					if (md.getBib().getPiece().getIssue() != null || md.getBib().getPiece().getPartName() != null) {
						query += " +piecedt:\""
								+ (md.getBib().getPiece().getIssue() != null ? md.getBib().getPiece().getIssue()
										: md.getBib().getPiece().getPartName())
								+ "\"";
					}
				}

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
			publisher(objectIdentifier, f.getAbsolutePath(), md, magXsd, admd);
			fSolr = new File(f.getAbsolutePath() + ".solr");
			admd.write(fSolr);
			admd.publish(Configuration.getValue("solr.batchPost"), fSolr);
			log.info("[" + QuartzTools.getName(context) + "] Fine pubblicazione");
			msg = "[" + QuartzTools.getName(context) + "] terminato regolarmente";
		} catch (SchedulerException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (XsdException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (IOException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		}
		return msg;
	}

	protected void publisher(String objectIdentifier, String filename, Metadigit md, MagXsd mdXsd,
			IndexDocumentTeca admd) throws SolrException {

		try {
			params = new Params();
			params.getParams().clear();

			params.add(ItemTeca.ID, objectIdentifier);
			// params.add(ItemTeca._ROOT_, objectIdentifier);
			params.add(ItemTeca.TIPOOGGETTO, ItemTeca.TIPOOGGETTO_DOCUMENTO);
			params.add(ItemTeca.TIPOLOGIAFILE, ItemTeca.TIPOLOGIAFILE_MAG);
			if (folder.getTipoRisorsa() != null) {
				params.add(ItemTeca.TIPORISORSA, folder.getTipoRisorsa());
			} else {
				read(ItemTeca.TIPORISORSA, md.getBib().getType());
			}
			if (folder.getFondo() != null) {
				params.add(ItemTeca.FONDO, folder.getFondo());
			}
			params.add(ItemTeca.ORIGINALFILENAME, filename);

			if (md.getBib().getLevel() != null) {
				params.add(ItemTeca.TIPODOCUMENTO,
						(md.getBib().getLevel().equals(BibliographicLevel.M) ? ItemTeca.TIPODOCUMENTO_LIBRODIGITALIZZATO
								: ItemTeca.TIPODOCUMENTO_PERIODICODIGITALIZZATO));
			}
			read(ItemTeca.BID, md.getBib().getIdentifier());
			read(ItemTeca.TITOLO, md.getBib().getTitle());
			read(ItemTeca.AUTORE, md.getBib().getCreator());
			read(ItemTeca.PUBBLICAZIONE, md.getBib().getPublisher());
			read(ItemTeca.SOGGETTO, md.getBib().getSubject());
			read(ItemTeca.DESCRIZIONE, md.getBib().getDescription());
			read(ItemTeca.CONTRIBUTO, md.getBib().getContributor());
			read(ItemTeca.DATA, md.getBib().getDate());
			read(ItemTeca.FORMATO, md.getBib().getFormat());
			read(ItemTeca.FONTE, md.getBib().getSource());
			read(ItemTeca.LINGUA, md.getBib().getLanguage());
			read(ItemTeca.RELAZIONE, md.getBib().getRelation());
			read(ItemTeca.COPERTURA, md.getBib().getCoverage());
			read(ItemTeca.GESTIONEDIRITTI, md.getBib().getRights());

			read(md.getBib().getPiece());
			read(md.getBib().getHoldings());

			params.add(ItemTeca.XML, mdXsd.write(md, new MagNamespacePrefix(), null, null, null));

			admd.add(params.getParams(), new ItemTeca());
		} catch (SolrException e) {
			throw e;
		} catch (XsdException e) {
			throw new SolrException(e.getMessage(), e);
		}
	}

	private void read(String key, List<SimpleLiteral> values) {
		List<String> sValues = null;
		if (values != null) {
			for (int x = 0; x < values.size(); x++) {
				sValues = ((SimpleLiteral) values.get(x)).getContent();
				for (int y = 0; y < sValues.size(); y++) {
					if (key.equals(ItemTeca.BID)) {
						params.add(key, sValues.get(y).replace("\\", ""));
					} else {
						params.add(key, sValues.get(y));
					}
				}
			}
		}
	}

	private void read(Piece value) {
		String stpieceper = null;
		String testo = null;
		int pos = 0;
		String[] st = null;

		if (value != null) {
			if (value.getYear() != null || value.getPartNumber() != null) {
				params.add(ItemTeca.PIECEGR,
						(value.getYear() != null ? value.getYear() : value.getPartNumber().toString()));
			}
			if (value.getIssue() != null || value.getPartName() != null) {
				params.add(ItemTeca.PIECEDT, (value.getIssue() != null ? value.getIssue() : value.getPartName()));
			}
			if (value.getStpiecePer() != null || value.getStpieceVol() != null) {
				params.add(ItemTeca.PIECEIN,
						(value.getStpiecePer() != null ? value.getStpiecePer() : value.getStpieceVol()));

				if (value.getStpiecePer() != null) {
					stpieceper = value.getStpiecePer();
					if (stpieceper.startsWith("(")) {
						stpieceper = stpieceper.substring(1);
						pos = stpieceper.indexOf(")");
						if (pos > -1) {
							testo = stpieceper.substring(0, pos);
							stpieceper = stpieceper.substring(pos + 1);
							if (testo.length() > 4) {
								testo = testo.substring(4);
								if (testo.length() > 2) {
									params.add(ItemTeca.PIECEMESE, testo.substring(0, 2));
									params.add(ItemTeca.PIECEMESEDESCR, convertMese(testo.substring(0, 2)));
									testo = testo.substring(2);
									if (testo.length() > 2) {
										params.add(ItemTeca.PIECEGIORNO, testo.substring(0, 2));
									} else {
										params.add(ItemTeca.PIECEGIORNO, testo);
									}
								} else {
									params.add(ItemTeca.PIECEMESE, testo);
									params.add(ItemTeca.PIECEMESEDESCR, convertMese(testo));
								}
							}
						}
					}
					if (stpieceper.length()>0) {
						st = stpieceper.split(":");
						params.add(ItemTeca.PIECEANNATA, st[0]);
					}
				}
			}
		}
	}

	private String convertMese(String numero) {
		Hashtable<String, String> mesi = null;

		mesi = new Hashtable<String, String>();
		mesi.put("01", "gennaio");
		mesi.put("02", "febbraio");
		mesi.put("03", "marzo");
		mesi.put("04", "aprile");
		mesi.put("05", "maggio");
		mesi.put("06", "giugno");
		mesi.put("07", "luglio");
		mesi.put("08", "agosto");
		mesi.put("09", "settembre");
		mesi.put("10", "ottobre");
		mesi.put("11", "novembre");
		mesi.put("12", "dicembre");
		mesi.put("21", "Primavera");
		mesi.put("22", "Estate");
		mesi.put("23", "Autunno");
		mesi.put("24", "Inverno");
		mesi.put("31", "primo quarto");
		mesi.put("32", "secondo quarto");
		mesi.put("33", "terzo quarto");
		mesi.put("34", "quarto quarto");
		
		if (mesi.get(numero)==null) {
			return numero;
		} else {
			return mesi.get(numero);
		}
	}

	private void read(List<Holdings> values) {
		for (int x = 0; x < values.size(); x++) {
			if (values.get(x).getInventoryNumber() != null) {
				params.add(ItemTeca.INVENTARIO, values.get(x).getInventoryNumber());
			}
			if (values.get(x).getLibrary() != null) {
				params.add(ItemTeca.BIBLIOTECA, values.get(x).getLibrary());
			}
			if (values.get(x).getShelfmark() != null) {
				for (int y = 0; y < values.get(x).getShelfmark().size(); y++) {
					params.add(ItemTeca.COLLOCAZIONE, values.get(x).getShelfmark().get(y).getContent());
				}
			}

		}
	}
}
