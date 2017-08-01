/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.san;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
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
public class JFondo extends JFile<String, MagXsd> {

	public static String COMPLESSOARCHIVISTICO = "complessoArchivistico";

	public static String IDSOGETTOCONSERVATORE = "idSoggettoConservatore";

	public static String SOGGETTOCONVERVATORE = "soggettoConservatore";

	private JobExecutionContext context = null;

	private Logger log = Logger.getLogger(JFondo.class);

	private ComplessoArchivistico complessoArchivistico = null;

	private String idSoggettoConservatore = null;

	private String soggettoConservatore = null;

	/**
	 * 
	 */
	public JFondo() {
	}

	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String msg = null;
		String objectIdentifier = null;
		IndexDocumentTeca admd = null;
		File fFondo = null;
		File fSolr = null;
		String key = "";

		try {
			this.context = context;
			complessoArchivistico = (ComplessoArchivistico) context.getJobDetail().getJobDataMap()
					.get(COMPLESSOARCHIVISTICO);
			idSoggettoConservatore = (String) context.getJobDetail().getJobDataMap().get(IDSOGETTOCONSERVATORE);
			soggettoConservatore = (String) context.getJobDetail().getJobDataMap().get(SOGGETTOCONVERVATORE);

			if (complessoArchivistico.getComplessoArchivisticoMadre() != null) {
				if (complessoArchivistico.getTipologia().equals("subFondo")) {
					key = idSoggettoConservatore + "." + complessoArchivistico.getComplessoArchivisticoMadre().getId()
							+ "." + complessoArchivistico.getId();
				} else if (complessoArchivistico.getTipologia().equals("serie")) {
					if (Configuration.getValue("serie.compile.union").equalsIgnoreCase("true")) {
						key = idSoggettoConservatore + "."
								+ complessoArchivistico.getComplessoArchivisticoMadre().getComplessoArchivisticoMadre()
										.getId()
								+ complessoArchivistico.getComplessoArchivisticoMadre().getId() + "."
								+ complessoArchivistico.getId();
					} else {
						key = complessoArchivistico.getId();
					}
				}
			} else {
				if (Configuration.getValue("fondo.compile.union").equalsIgnoreCase("true")) {
					key = idSoggettoConservatore + "." + complessoArchivistico.getId();
				} else {
					key = complessoArchivistico.getId();
				}
			}
			admd = new IndexDocumentTeca();

			objectIdentifier = findId(context, key);

			fFondo = new File(Configuration.getValue("pathFondo") + File.separator + key + ".xml");
			publisher(objectIdentifier, null, key, null, admd);

			fSolr = new File(fFondo.getAbsolutePath() + ".solr");
			admd.write(fSolr);
			admd.publish(Configuration.getValue("solr.batchPost"), fSolr);

			msg = "["+QuartzTools.getName(context)+"] terminato regolarmente";
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
	@SuppressWarnings("unchecked")
	@Override
	protected void publisher(String objectIdentifier, String filename, String idFondo, MagXsd mdXsd,
			IndexDocumentTeca admd) throws SolrException, FileNotFoundException {
		// Vector<String> soggettiProtuttori= null;
		Enumeration<String> keys = null;
		String key = null;
		Vector<String> childrens = null;
		String[] st = null;
		Hashtable<String, ComplessoArchivistico> complessiArchivistici = null;
		try {
			System.out.println("idSoggettoConservatore: " + idSoggettoConservatore + "\tSoggettoConservatore: "
					+ soggettoConservatore + "\tidFondo: " + idFondo + "\tFondo: "
					+ complessoArchivistico.getDenominazione());
			params = new Params();
			params.getParams().clear();

			params.add(ItemTeca.ID, objectIdentifier);
			params.add(ItemTeca.TIPOOGGETTO, ItemTeca.TIPOOGGETTO_COMPLESSOARCHIVISTICO);
			params.add(ItemTeca.TIPOLOGIAFILE, ItemTeca.TIPOLOGIAFILE_COMPLESSOARCHIVISTICO);

			read(ItemTeca.BID, idFondo);
			read(ItemTeca.TITOLO, complessoArchivistico.getDenominazione());
			read(ItemTeca.TIPOLOGIA, complessoArchivistico.getTipologia());
			read(ItemTeca.ESTREMI, complessoArchivistico.getData());
			read(ItemTeca.CONSISTENZACARTE, complessoArchivistico.getConsistenza());
			read(ItemTeca.CONSISTENZASAST, complessoArchivistico.getConsistenzaSast());
			read(ItemTeca.DESCRIZIONE, complessoArchivistico.getDescrizione());

			read(ItemTeca.SISTEMAADERENTE, complessoArchivistico.getSistemaAderente());
			read(ItemTeca.SCHEDAPROVENIENZAURL, complessoArchivistico.getSchedaProvenienza());

			read(ItemTeca.SOGGETTOCONSERVATOREKEY, idSoggettoConservatore);
			read(ItemTeca.SOGGETTOCONSERVATORE, soggettoConservatore);

			if (complessoArchivistico.getComplessoArchivisticoMadre() != null) {
				if (complessoArchivistico.getTipologia().equals("subFondo")) {
					read(ItemTeca.FONDOKEY, complessoArchivistico.getComplessoArchivisticoMadre().getId());
					read(ItemTeca.FONDO, complessoArchivistico.getComplessoArchivisticoMadre().getDenominazione());
					read(ItemTeca.FONDOSCHEDA, "Si");
				} else if (complessoArchivistico.getTipologia().equals("serie")) {
					if (Configuration.getValue("serie.compile.union").equalsIgnoreCase("true")) {
						read(ItemTeca.FONDOKEY, complessoArchivistico.getComplessoArchivisticoMadre()
								.getComplessoArchivisticoMadre().getId());
						read(ItemTeca.FONDO, complessoArchivistico.getComplessoArchivisticoMadre()
								.getComplessoArchivisticoMadre().getDenominazione());
						read(ItemTeca.FONDOSCHEDA, "Si");
						read(ItemTeca.SUBFONDOKEY, complessoArchivistico.getComplessoArchivisticoMadre().getId());
						read(ItemTeca.SUBFONDO,
								complessoArchivistico.getComplessoArchivisticoMadre().getDenominazione());
						read(ItemTeca.SUBFONDOSCHEDA,
								idSoggettoConservatore + "."
										+ complessoArchivistico.getComplessoArchivisticoMadre()
												.getComplessoArchivisticoMadre().getId()
										+ "." + complessoArchivistico.getComplessoArchivisticoMadre().getId());
					} else {
						read(ItemTeca.FONDOKEY, complessoArchivistico.getComplessoArchivisticoMadre().getId());
						read(ItemTeca.FONDO, complessoArchivistico.getComplessoArchivisticoMadre().getDenominazione());
						read(ItemTeca.FONDOSCHEDA, "Si");
					}
				}
			}

			if (complessoArchivistico.getSoggettiProduttori() != null
					&& complessoArchivistico.getSoggettiProduttori().size() > 0) {
				// soggettiProtuttori = (Vector<String>)
				// Configuration.getValues("fondo."+idFondo+".soggettoProduttore");
				// for (int x= 0; x<soggettiProtuttori.size(); x++){
				for (int x = 0; x < complessoArchivistico.getSoggettiProduttori().size(); x++) {
					read(ItemTeca.SOGGETTOPRODUTTOREKEY, complessoArchivistico.getSoggettiProduttori().get(x).getId());
					read(ItemTeca.SOGGETTOPRODUTTORE,
							complessoArchivistico.getSoggettiProduttori().get(x).getFormaAutorizzata());
//					checkJobs(context);
					start(complessoArchivistico.getSoggettiProduttori().get(x),
							idSoggettoConservatore + "." + complessoArchivistico.getId(),
							complessoArchivistico.getDenominazione(), context);
				}
			}

			st = idFondo.split("\\.");
			if (complessoArchivistico.getTipologia().equals("fondo")) {
				if (Configuration.getValue("fondo.compile.union").equalsIgnoreCase("true")) {
					childrens = findSoggettoConservatoreFigli(context, st[0], st[1], null);
				} else {
					childrens = findSoggettoConservatoreFigli(context, idSoggettoConservatore, idFondo, null);
				}
			} else if (complessoArchivistico.getTipologia().equals("subFondo")) {
				childrens = findSoggettoConservatoreFigli(context, st[0], st[1], st[2]);
			} else if (complessoArchivistico.getTipologia().equals("serie")) {
				if (Configuration.getValue("serie.compile.union").equalsIgnoreCase("false")) {
					childrens = findSoggettoConservatoreFigli(context, idSoggettoConservatore, idFondo, null);
				}
			}

			if (childrens != null) {
				if (complessoArchivistico.getComplessoArchivisticoFigli() != null) {
					complessiArchivistici = (Hashtable<String, ComplessoArchivistico>) complessoArchivistico
							.getComplessoArchivisticoFigli().clone();
					for (int x = 0; x < childrens.size(); x++) {
						st = childrens.get(x).split("\\$");
						read(ItemTeca.CHILDREN, st[0]);
						read(ItemTeca.CHILDRENDESC, st[1]);
						if (complessiArchivistici != null && complessiArchivistici.get(st[0]) != null) {
//							checkJobs(context);
							start(complessiArchivistici.get(st[0]), idSoggettoConservatore,
									soggettoConservatore, context);
							complessiArchivistici.remove(st[0]);
						}
					}
					if (complessiArchivistici != null) {
						keys = complessiArchivistici.keys();
						while (keys.hasMoreElements()) {
							key = keys.nextElement();

							read(ItemTeca.CHILDREN, key);
							read(ItemTeca.CHILDRENDESC, complessiArchivistici.get(key).getDenominazione());
//							checkJobs(context);
							start(complessiArchivistici.get(key), idFondo,
									complessoArchivistico.getDenominazione(), context);
						}
					}
				}
			}
			admd.add(params.getParams(), new ItemTeca());
		} catch (SolrException e) {
			throw e;
		} catch (SchedulerException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new SolrException(e.getMessage(), e);
		} catch (ConfigurationException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new SolrException(e.getMessage(), e);
		}
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

	private Vector<String> findSoggettoConservatoreFigli(JobExecutionContext context, String idIstituto, String idFondo,
			String idSubFondo) throws JobExecutionException {
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		// String objectIdentifier = null;
		String[] facetField = null;
		NamedList<List<PivotField>> facetPivot = null;
		List<PivotField> lPivotField = null;

		String key = null;
		String descr = null;
		Vector<String> result = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
					Configuration.getValue("solr.collection"),
					Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
			query = "+" + ItemTeca.SOGGETTOCONSERVATOREKEY + ":\"" + idIstituto + "\" " + "+" + ItemTeca.FONDOKEY
					+ ":\"" + idFondo + "\" ";

			if (idSubFondo != null) {
				query += " +" + ItemTeca.SUBFONDOKEY + ":\"" + idSubFondo + "\" ";
			}
			facetField = new String[1];
			if (idSubFondo != null) {
				facetField[0] = "subFondo2_fc,subFondo2Key_fc";
			} else {
				facetField[0] = "subFondo_fc,subFondoKey_fc";
			}
			find.enableFacetPivot(1, -1, FacetParams.FACET_SORT_INDEX, facetField);
			qr = find.find(query, 0, 1);
			if (qr.getResponse() != null && qr.getResponse().get("response") != null) {
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound() > 0) {
					facetPivot = qr.getFacetPivot();
					if (facetPivot != null) {
						for (int x = 0; x < facetPivot.size(); x++) {
							lPivotField = facetPivot.getVal(x);
							for (int y = 0; y < lPivotField.size(); y++) {
								descr = null;
								key = null;

								descr = ((String) lPivotField.get(y).getValue()).replace("_", " ");
								if (lPivotField.get(y).getPivot() != null) {
									for (int z = 0; z < lPivotField.get(y).getPivot().size(); z++) {

										key = (String) lPivotField.get(y).getPivot().get(z).getValue();
									}
								}
								if (key != null) {
									if (result == null) {
										result = new Vector<String>();
									}
									result.add(key + "$" + descr);
								}
							}
						}
					}
					// objectIdentifier = (String)
					// response.get(0).get(ItemTeca.ID);
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
		// if (objectIdentifier == null){
		// objectIdentifier = UUID.randomUUID().toString();
		// }
		return result;
	}

}
