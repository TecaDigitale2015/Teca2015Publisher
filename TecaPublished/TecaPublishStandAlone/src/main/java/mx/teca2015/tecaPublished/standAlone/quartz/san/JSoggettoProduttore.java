/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.san;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.mag.MagXsd;
import mx.randalf.quartz.QuartzTools;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JFile;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class JSoggettoProduttore extends JFile<String, MagXsd> {

	public static String SOGGETTOPRODUTTORE = "soggettoProduttore";

	public static String IDCOMPLESSOARCHIVISTICO = "idComplessoArchivistico";

	public static String COMPLESSOARCHIVISTICO = "complessoArchivistico";

	private JobExecutionContext context = null;

	private Logger log = Logger.getLogger(JSoggettoProduttore.class);

	private SoggettoProduttore soggettoProduttore = null;

	private String idComplessoArchivistico = null;

	private String complessoArchivistico = null;

	/**
	 * 
	 */
	public JSoggettoProduttore() {
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String objectIdentifier = null;
		IndexDocumentTeca admd = null;
		String key = "";
		File fFondo = null;
		File fSolr = null;
		String msg = null;

		try {
			this.context = context;

			soggettoProduttore = (SoggettoProduttore) context.getJobDetail().getJobDataMap().get(SOGGETTOPRODUTTORE);
			idComplessoArchivistico = (String) context.getJobDetail().getJobDataMap().get(IDCOMPLESSOARCHIVISTICO);
			complessoArchivistico = (String) context.getJobDetail().getJobDataMap().get(COMPLESSOARCHIVISTICO);

			key = soggettoProduttore.getId();
			admd = new IndexDocumentTeca();

			objectIdentifier = findId(context, key);

			fFondo = new File(Configuration.getValue("pathSoggettoProduttore") + File.separator + key + ".xml");
			publisher(objectIdentifier, null, key, null, admd);
			fSolr = new File(fFondo.getAbsolutePath() + ".solr");
			admd.write(fSolr);
			admd.publish(Configuration.getValue("solr.batchPost"), fSolr);
			msg = "Processo: +" + context.getJobDetail().getKey().getGroup() + " => "
					+ context.getJobDetail().getKey().getName() + " => " + context.getTrigger().getKey().getGroup()
					+ " => " + context.getTrigger().getKey().getName() + " terminato regolarmente";
		} catch (FileNotFoundException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SchedulerException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (IOException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new JobExecutionException(e.getMessage(), e, false);
		}
		return msg;
	}

	/**
	 * @see mx.teca2015.tecaPublished.standAlone.quartz.file.JFile#publisher(java.lang.String,
	 *      java.lang.String, java.lang.Object, java.lang.Object,
	 *      mx.teca2015.tecaUtility.solr.IndexDocumentTeca)
	 */
	@Override
	protected void publisher(String objectIdentifier, String filename, String md, MagXsd mdXsd, IndexDocumentTeca admd)
			throws SolrException, FileNotFoundException {
		Hashtable<String, String> fondi = null;
		Enumeration<String> keys = null;
		String key = null;
		boolean trovato = false;

		try {
			System.out.println("idSoggettoProduttore: " + soggettoProduttore.getId() + "\tSoggettoProduttore: "
					+ soggettoProduttore.getFormaAutorizzata());
			params = new Params();
			params.getParams().clear();

			params.add(ItemTeca.ID, objectIdentifier);
			params.add(ItemTeca.TIPOOGGETTO, ItemTeca.TIPOOGGETTO_SOGGETTOPRODUTTORE);
			params.add(ItemTeca.TIPOLOGIAFILE, ItemTeca.TIPOLOGIAFILE_SOGGETTOPRODUTTORE);

			read(ItemTeca.BID, soggettoProduttore.getId());
			read(ItemTeca.TIPOLOGIA, soggettoProduttore.getTipologia());
			read(ItemTeca.TITOLO, soggettoProduttore.getFormaAutorizzata());
			read(ItemTeca.ALTREDENOMINAZIONI, soggettoProduttore.getAltreDenominazioni());
			read(ItemTeca.DATAESISTENZA, soggettoProduttore.getDataEsistenza());
			read(ItemTeca.DATAMORTE, soggettoProduttore.getDataMorte());
			read(ItemTeca.LUOGONASCITA, soggettoProduttore.getLuogoNascita());
			read(ItemTeca.LUOGOMORTE, soggettoProduttore.getLuogoMorte());
			read(ItemTeca.SEDE, soggettoProduttore.getSede());
			read(ItemTeca.NATURAGIURIDICA, soggettoProduttore.getNaturaGiuridica());
			read(ItemTeca.TIPOENTE, soggettoProduttore.getTipoEnte());
			read(ItemTeca.AMBITOTERRITORIALE, soggettoProduttore.getAmbitoTerritoriale());
			read(ItemTeca.TITOLOSP, soggettoProduttore.getTitolo());
			read(ItemTeca.DESCRIZIONE, soggettoProduttore.getDescrizione());
			read(ItemTeca.SISTEMAADERENTE, soggettoProduttore.getSistemaAderente());
			read(ItemTeca.SCHEDAPROVENIENZAURL, soggettoProduttore.getSchedaProvenienza());

			fondi = findFondiByBid(context, soggettoProduttore.getId());
			keys = fondi.keys();
			while (keys.hasMoreElements()) {
				key = keys.nextElement();

				if (key.equals(idComplessoArchivistico)) {
					read(ItemTeca.FONDOKEY, idComplessoArchivistico);
					read(ItemTeca.FONDO, complessoArchivistico);
					trovato = true;
				} else {
					read(ItemTeca.FONDOKEY, key);
					read(ItemTeca.FONDO, fondi.get(key));
				}
			}
			if (!trovato) {
				read(ItemTeca.FONDOKEY, idComplessoArchivistico);
				read(ItemTeca.FONDO, complessoArchivistico);
			}

			admd.add(params.getParams(), new ItemTeca());
		} catch (JobExecutionException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new SolrException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private Hashtable<String, String> findFondiByBid(JobExecutionContext context, String id)
			throws JobExecutionException {
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		Hashtable<String, String> fondi = new Hashtable<String, String>();
		ArrayList<String> fondiKeys = null;
		ArrayList<String> fondis = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
					Configuration.getValue("solr.collection"),
					Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
			query = "bid:\"" + id + "\"";

			qr = find.find(query);
			if (qr.getResponse() != null && qr.getResponse().get("response") != null) {
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound() > 0) {
					fondiKeys = ((ArrayList<String>) response.get(0).get(ItemTeca.FONDOKEY));
					fondis = ((ArrayList<String>) response.get(0).get(ItemTeca.FONDO));
					if (fondiKeys != null) {
						for (int x = 0; x < fondiKeys.size(); x++) {
							fondi.put(fondiKeys.get(x), fondis.get(x));
						}
					}
				}
			}
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
		} finally {
			try {
				if (find != null) {
					find.close();
				}
			} catch (IOException e) {
				log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
		return fondi;
	}

}
