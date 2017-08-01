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
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
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
import mx.randalf.solr.Item;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JFile;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class JIstituto extends JFile<SoggettoConservatore, MagXsd> {

	private Logger log = Logger.getLogger(JIstituto.class);

	public static String SOGGETTOCONSERVATORE = "soggettoConservatore";

	private JobExecutionContext context = null;

	/**
	 * 
	 */
	public JIstituto() {
	}

	/**
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected String jobExecute(JobExecutionContext context) throws JobExecutionException {
		String msg = null;
		SoggettoConservatore soggettoConservatore = null;
		Vector<ComplessoArchivistico> complessiArchivistici = null;
		String objectIdentifier = null;
		IndexDocumentTeca admd = null;
		File fSolr = null;
		File fIstituto = null;
		Vector<String> idComplessoArchivistico = null;
		Vector<String[]> idSubFondo = null;
		Vector<String[]> idSerie = null;
		String[] dati = null;
		Hashtable<String, ComplessoArchivistico> subFondo = null;
		Enumeration<String> keys = null;
		String key = null;
		Hashtable<String, ComplessoArchivistico> subFondo2 = null;
		Enumeration<String> keys2 = null;
		String key2 = null;

		try {
			this.context = context;
			soggettoConservatore = (SoggettoConservatore) context.getJobDetail().getJobDataMap()
					.get(SOGGETTOCONSERVATORE);

			admd = new IndexDocumentTeca();

			fIstituto = new File(
					Configuration.getValue("pathIstituti") + File.separator + soggettoConservatore.getId() + ".xml");

			objectIdentifier = findId(context, soggettoConservatore.getId());

			publisher(objectIdentifier, null, soggettoConservatore, null, admd);

			if (soggettoConservatore.getComplessiArchivistici() != null) {
				complessiArchivistici = (Vector<ComplessoArchivistico>) soggettoConservatore.getComplessiArchivistici()
						.clone();

				if (complessiArchivistici != null && complessiArchivistici.size() > 0) {
					idComplessoArchivistico = new Vector<String>();
					idSubFondo = new Vector<String[]>();
					idSerie = new Vector<String[]>();

					for (int x = 0; x < complessiArchivistici.size(); x++) {
						// soggettoConservatore.getComplessiArchivistici().get(x).getSoggettiProduttori().get(0)ComplessoArchivisticoFigli().get(0)
						if (complessiArchivistici.get(x).getTipologia().equals("fondo")) {
							idComplessoArchivistico.add(complessiArchivistici.get(x).getId());

							if (complessiArchivistici.get(x).getComplessoArchivisticoFigli() != null) {
								subFondo = complessiArchivistici.get(x).getComplessoArchivisticoFigli();
								keys = subFondo.keys();
								while (keys.hasMoreElements()) {
									key = keys.nextElement();

									if (subFondo.get(key).getTipologia().equals("subFondo")) {
										dati = new String[2];
										dati[0] = complessiArchivistici.get(x).getId();
										dati[1] = key;
										idSubFondo.add(dati);

										if (subFondo.get(key).getComplessoArchivisticoFigli() != null) {
											subFondo2 = subFondo.get(key).getComplessoArchivisticoFigli();
											keys2 = subFondo2.keys();
											while (keys2.hasMoreElements()) {
												key2 = keys2.nextElement();

												if (subFondo2.get(key2).getTipologia().equals("serie")) {
													dati = new String[3];
													dati[0] = complessiArchivistici.get(x).getId();
													dati[1] = key;
													dati[2] = key2;
													idSerie.add(dati);
												}
											}
										}
									} else if (subFondo.get(key).getTipologia().equals("serie")) {
										dati = new String[2];
										dati[0] = complessiArchivistici.get(x).getId();
										dati[1] = key;
										idSubFondo.add(dati);

										if (subFondo.get(key).getComplessoArchivisticoFigli() != null) {
											subFondo2 = subFondo.get(key).getComplessoArchivisticoFigli();
											keys2 = subFondo2.keys();
											while (keys2.hasMoreElements()) {
												key2 = keys2.nextElement();

												if (subFondo2.get(key2).getTipologia().equals("serie")) {
													dati = new String[3];
													dati[0] = complessiArchivistici.get(x).getId();
													dati[1] = key;
													dati[2] = key2;
													idSerie.add(dati);
												}
											}
										}
									}
								}
							}
						}
					}
				}
			} else {
				System.out.println("Mancano i Complessi Archivistici");
			}
			aggSoggettoConservtore(soggettoConservatore.getId(), idComplessoArchivistico, idSubFondo, idSerie, admd);

			fSolr = new File(fIstituto.getAbsolutePath() + ".solr");
			admd.write(fSolr);
			admd.publish(Configuration.getValue("solr.batchPost"), fSolr);

			waithEndJobs(context);
			msg = "[" + QuartzTools.getName(context) + "] terminato regolarmente";
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
		/*
		 * http://mxmuletto.bncf.lan:8983/solr/tecaPuglia/select?q=
		 * soggettoConservatoreKey%3AASBA&rows=1&wt=xml&indent=true&facet=true&
		 * facet.pivot=fondo_fc%2CfondoKey_fc&facet.sort=index&facet.limit=-1
		 * http://mxmuletto.bncf.lan:8983/solr/tecaPuglia/select?
		 * q=soggettoConservatoreKey%3AASBA& rows=1& wt=xml&
		 * indent=true&facet=true&
		 * facet.pivot=fondo_fc%2CfondoKey_fc,soggettoConservatoreKey_fc&
		 * facet.pivot=soggettoConservatore_fc,soggettoConservatoreKey_fc&
		 * facet.sort=index& facet.limit=-1
		 */
		return msg;
	}

	@Override
	protected void publisher(String objectIdentifier, String filename, SoggettoConservatore soggettoConservatore,
			MagXsd mdXsd, IndexDocumentTeca admd) throws SolrException, FileNotFoundException {
		DatiIstituto datiIstituto = null;
		String[] st = null;
		Vector<ComplessoArchivistico> complessiArchivistici = null;
		Hashtable<String, Integer> keyCompArch = null;
		Enumeration<String> keys = null;
		String key = null;
		String telefono = null;

		try {
			datiIstituto = findSoggettoConservatoreKey(context, soggettoConservatore.getId());
			params = new Params();
			params.getParams().clear();

			params.add(ItemTeca.ID, objectIdentifier);
			params.add(ItemTeca.TIPOOGGETTO, ItemTeca.TIPOOGGETTO_SOGGETTOCONSERVATORE);
			params.add(ItemTeca.TIPOLOGIAFILE, ItemTeca.TIPOLOGIAFILE_SOGGETTOCONSERVATORE);

			read(ItemTeca.BID, soggettoConservatore.getId());
			read(ItemTeca.TITOLO, soggettoConservatore.getDenominazione());
			read(ItemTeca.TIPOSOGGETTOCONSERVATORE, soggettoConservatore.getTipoSoggettoConservatore());
			read(ItemTeca.DESCRIZIONE, soggettoConservatore.getDescrizione());
			read(ItemTeca.INDIRIZZO, soggettoConservatore.getIndirizzo());
			telefono = soggettoConservatore.getTelefono();
			if (telefono != null && !telefono.trim().equals("")) {
				telefono = telefono.replace("-", "");
				st = telefono.split(" ");
				for (int x = 0; x < st.length; x++) {
					if (!st[x].trim().equals("")) {
						read(ItemTeca.TELEFONO, st[x]);
					}
				}
			}
			read(ItemTeca.FAX, soggettoConservatore.getCelllulare());
			read(ItemTeca.EMAIL, soggettoConservatore.getEmail());
			read(ItemTeca.SERVIZIOPUB, soggettoConservatore.getServizioConsultazioneAlPubblico());
			read(ItemTeca.ORARIOAPERTURA, soggettoConservatore.getOrarioApertura());

			if (soggettoConservatore.getSchedeConservatori() != null
					&& soggettoConservatore.getSchedeConservatori().size() > 0) {
				for (int x = 0; x < soggettoConservatore.getSchedeConservatori().size(); x++) {
					read(ItemTeca.SCHEDECONSERVATORI, soggettoConservatore.getSchedeConservatori().get(x).getTesto());
					if (soggettoConservatore.getSchedeConservatori().get(x).getUrl() != null
							&& !soggettoConservatore.getSchedeConservatori().get(x).getUrl().trim().equals("")) {
						read(ItemTeca.SCHEDECONSERVATORIURL,
								soggettoConservatore.getSchedeConservatori().get(x).getUrl());
					} else {
						read(ItemTeca.SCHEDECONSERVATORIURL, "none");
					}
				}
			}

			if (soggettoConservatore.getRisorseEsterne() != null
					&& soggettoConservatore.getRisorseEsterne().size() > 0) {
				for (int x = 0; x < soggettoConservatore.getRisorseEsterne().size(); x++) {
					read(ItemTeca.RISORSEESTERNE, soggettoConservatore.getRisorseEsterne().get(x).getTesto());
					if (soggettoConservatore.getRisorseEsterne().get(x).getUrl() != null
							&& !soggettoConservatore.getRisorseEsterne().get(x).getUrl().trim().equals("")) {
						read(ItemTeca.RISORSEESTERNEURL, soggettoConservatore.getRisorseEsterne().get(x).getUrl());
					} else {
						read(ItemTeca.RISORSEESTERNEURL, "none");
					}
				}
			}

			complessiArchivistici = soggettoConservatore.getComplessiArchivistici();
			keyCompArch = new Hashtable<String, Integer>();
			if (complessiArchivistici != null) {
				for (int x = 0; x < complessiArchivistici.size(); x++) {
					keyCompArch.put(complessiArchivistici.get(x).getId(), x);
				}
			}

			if (datiIstituto != null && datiIstituto.getFondi() != null) {
				for (int x = 0; x < datiIstituto.getFondi().size(); x++) {
					st = datiIstituto.getFondi().get(x).split("\\$");
					if (keyCompArch.get(st[0]) != null) {
						if (complessiArchivistici.get(keyCompArch.get(st[0])).getTipologia().equals("fondo")) {
							read(ItemTeca.CHILDRENDESC, st[1]);
							read(ItemTeca.CHILDREN, st[0]);
						}
						start(complessiArchivistici.get(keyCompArch.get(st[0])),
								soggettoConservatore.getId(), soggettoConservatore.getDenominazione(), context);
						keyCompArch.remove(st[0]);
					}
				}
			}
			if (keyCompArch != null && keyCompArch.size() > 0) {
				keys = keyCompArch.keys();
				while (keys.hasMoreElements()) {
					key = keys.nextElement();
					if (complessiArchivistici.get(keyCompArch.get(key)).getTipologia().equals("fondo")) {
						read(ItemTeca.CHILDREN, key);
						read(ItemTeca.CHILDRENDESC, complessiArchivistici.get(keyCompArch.get(key)).getDenominazione());
					}
					start(complessiArchivistici.get(keyCompArch.get(key)), soggettoConservatore.getId(),
							soggettoConservatore.getDenominazione(), context);
				}
			}
			admd.add(params.getParams(), new ItemTeca());
		} catch (JobExecutionException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new SolrException(e.getMessage(), e);
		} catch (SchedulerException e) {
			log.error("[" + QuartzTools.getName(context) + "] " + e.getMessage(), e);
			throw new SolrException(e.getMessage(), e);
		}
	}

	private DatiIstituto findSoggettoConservatoreKey(JobExecutionContext context, String idIstituto)
			throws JobExecutionException {
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		DatiIstituto datiIstituto = null;
		// String objectIdentifier = null;
		String[] facetField = null;
		NamedList<List<PivotField>> facetPivot = null;
		List<PivotField> lPivotField = null;
		String fondo = null;
		String fondoKey = null;
		String istituto = null;
		String istitutoKey = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
					Configuration.getValue("solr.collection"),
					Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
			query = "soggettoConservatoreKey:\"" + idIstituto + "\"";

			facetField = new String[1];
			facetField[0] = "fondo_fc,fondoKey_fc,soggettoConservatoreKey_fc,soggettoConservatore_fc";
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
								fondo = null;
								fondoKey = null;
								istituto = null;
								istitutoKey = null;

								fondo = ((String) lPivotField.get(y).getValue()).replace("_", " ");
								if (lPivotField.get(y).getPivot() != null) {
									for (int z = 0; z < lPivotField.get(y).getPivot().size(); z++) {

										fondoKey = (String) lPivotField.get(y).getPivot().get(z).getValue();
										if (lPivotField.get(y).getPivot().get(z).getPivot() != null) {
											for (int a = 0; a < lPivotField.get(y).getPivot().get(z).getPivot()
													.size(); a++) {
												if (((String) lPivotField.get(y).getPivot().get(z).getPivot().get(a)
														.getValue()).equalsIgnoreCase(idIstituto)
														&& istitutoKey == null) {
													istitutoKey = (String) lPivotField.get(y).getPivot().get(z)
															.getPivot().get(a).getValue();
													if (lPivotField.get(y).getPivot().get(z).getPivot().get(a)
															.getPivot() != null) {
														for (int b = 0; b < lPivotField.get(y).getPivot().get(z)
																.getPivot().get(a).getPivot().size(); b++) {
															istituto = ((String) lPivotField.get(y).getPivot().get(z)
																	.getPivot().get(a).getPivot().get(b).getValue())
																			.replace("_", " ");
														}
													}
												}
											}
										}
									}
								}
								if (istitutoKey != null) {
									if (datiIstituto == null) {
										datiIstituto = new DatiIstituto(istituto);
									}
									datiIstituto.addFondo(fondoKey + "$" + fondo);
									// System.out.println("Fondo: "+fondo+
									// "\tFondoKey: "+fondoKey+
									// "\tIstituto: "+istituto+
									// "\tIstitutoKey: "+istitutoKey);
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
		return datiIstituto;
	}

	private void aggSoggettoConservtore(String id, Vector<String> idFondo, Vector<String[]> idSubFondo,
			Vector<String[]> idSerie, IndexDocumentTeca admd) throws JobExecutionException {
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int nSoggettoCons = 0;
		int nFondi = 0;
		int nSubFondi = 0;
		int nSubFondi2 = 0;

		String tipologiaFile = null;
		String soggettoConservatore = null;
		String soggettoConservatoreScheda = null;
		String fondo = null;
		String fondoScheda = null;
		String subFondo = null;
		String subFondoScheda = null;
		String subFondo2 = null;
		String subFondo2Scheda = null;
		String keySubFondo = "";
		String keySubFondo2 = "";

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration.getValue("solr.Cloud")),
					Configuration.getValue("solr.collection"),
					Integer.parseInt(Configuration.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration.getValue("solr.clientTimeOut")));
			query = ItemTeca.SOGGETTOCONSERVATOREKEY + ":\"" + id + "\""
			// +" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
			;

			qr = find.find(query, 0, 1000000);
			if (qr.getResponse() != null && qr.getResponse().get("response") != null) {
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound() > 0) {
					for (int x = 0; x < response.getNumFound(); x++) {
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String) ((ArrayList<Object>) doc
								.getFieldValues(ItemTeca.TIPOLOGIAFILE + "_show")).get(0));
						soggettoConservatore = ((String) ((ArrayList<Object>) doc
								.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY + "_show")).get(0));
						soggettoConservatoreScheda = (doc
								.get(ItemTeca.SOGGETTOCONSERVATORESCHEDA + "_show") == null
										? null
										: ((String) ((ArrayList<Object>) doc
												.getFieldValues(ItemTeca.SOGGETTOCONSERVATORESCHEDA + "_show"))
														.get(0)));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF)
								|| tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC)
								|| tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD)
								|| tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_BUSTE)) {

							if (soggettoConservatore.equals(id)) {
								if (soggettoConservatoreScheda == null) {
									if (params == null) {
										params = Item.convert(doc);
									}
									params.getParams().remove(ItemTeca.SOGGETTOCONSERVATORESCHEDA);
									params.add(ItemTeca.SOGGETTOCONSERVATORESCHEDA, "Si");
									nSoggettoCons++;
								} else if (soggettoConservatoreScheda.equals("")
										|| !soggettoConservatoreScheda.equals("Si")) {
									if (params == null) {
										params = Item.convert(doc);
									}
									params.getParams().remove(ItemTeca.SOGGETTOCONSERVATORESCHEDA);
									params.add(ItemTeca.SOGGETTOCONSERVATORESCHEDA, "Si");
									nSoggettoCons++;
								}

								if (idFondo != null && idFondo.size() > 0) {

									fondo = ((String) ((ArrayList<Object>) doc
											.getFieldValues(ItemTeca.FONDOKEY + "_show")).get(0));
									fondoScheda = (doc
											.get(ItemTeca.FONDOSCHEDA
													+ "_show") == null
															? null
															: ((String) ((ArrayList<Object>) doc
																	.getFieldValues(ItemTeca.FONDOSCHEDA + "_show"))
																			.get(0)));
									if (fondoScheda == null || fondoScheda.trim().equals("")) {
										for (int y = 0; y < idFondo.size(); y++) {
											if (fondo.equals(idFondo.get(y))) {
												if (fondoScheda == null) {
													if (params == null) {
														params = Item.convert(doc);
													}
													params.add(ItemTeca.FONDOSCHEDA, "Si");
													nFondi++;
												} else if (fondoScheda.equals("") || !fondoScheda.equals("Si")) {
													if (params == null) {
														params = Item.convert(doc);
													}
													params.getParams().remove(ItemTeca.FONDOSCHEDA);
													params.add(ItemTeca.FONDOSCHEDA, "Si");
													nFondi++;
												}
											}
										}
									}
								}

								if (idSubFondo != null && idSubFondo.size() > 0) {
									if (((ArrayList<Object>) doc
											.getFieldValues(ItemTeca.SUBFONDOKEY + "_show")) != null) {
										fondo = ((String) ((ArrayList<Object>) doc
												.getFieldValues(ItemTeca.FONDOKEY + "_show")).get(0));
										subFondo = ((String) ((ArrayList<Object>) doc
												.getFieldValues(ItemTeca.SUBFONDOKEY + "_show")).get(0));
										subFondoScheda = (doc
												.get(ItemTeca.SUBFONDOSCHEDA + "_show") == null
														? null
														: ((String) ((ArrayList<Object>) doc
																.getFieldValues(ItemTeca.SUBFONDOSCHEDA + "_show"))
																		.get(0)));
										// if (subFondoScheda== null ||
										// subFondoScheda.trim().equals("No")){
										for (int y = 0; y < idSubFondo.size(); y++) {
											if (fondo.trim().equals(idSubFondo.get(y)[0].trim())
													&& subFondo.trim().equals(idSubFondo.get(y)[1].trim())) {
												if (Configuration.getValue("subFondoScheda.compile.union")
														.equalsIgnoreCase("true")) {
													keySubFondo = soggettoConservatore + "." + fondo + "." + subFondo;
												} else {
													keySubFondo = subFondo;
												}
												if (subFondoScheda == null) {
													if (params == null) {
														params = Item.convert(doc);
													}
													params.add(ItemTeca.SUBFONDOSCHEDA, keySubFondo);
													nSubFondi++;
												} else if (subFondoScheda.equals("")
														|| !subFondoScheda.equals(keySubFondo)) {
													if (params == null) {
														params = Item.convert(doc);
													}
													params.getParams().remove(ItemTeca.SUBFONDOSCHEDA);
													params.add(ItemTeca.SUBFONDOSCHEDA, keySubFondo);
													nSubFondi++;
												}
											}
										}
										// }
									}
								}

								if (idSerie != null && idSerie.size() > 0) {
									if (((ArrayList<Object>) doc
											.getFieldValues(ItemTeca.SUBFONDO2KEY + "_show")) != null) {
										fondo = ((String) ((ArrayList<Object>) doc
												.getFieldValues(ItemTeca.FONDOKEY + "_show")).get(0));
										subFondo = ((String) ((ArrayList<Object>) doc
												.getFieldValues(ItemTeca.SUBFONDOKEY + "_show")).get(0));
										subFondo2 = ((String) ((ArrayList<Object>) doc
												.getFieldValues(ItemTeca.SUBFONDO2KEY + "_show")).get(0));
										subFondo2Scheda = (doc
												.get(ItemTeca.SUBFONDO2SCHEDA + "_show") == null
														? null
														: ((String) ((ArrayList<Object>) doc
																.getFieldValues(ItemTeca.SUBFONDO2SCHEDA + "_show"))
																		.get(0)));
										// if (subFondoScheda== null ||
										// subFondoScheda.trim().equals("No")){
										for (int y = 0; y < idSerie.size(); y++) {
											if (fondo.trim().equals(idSerie.get(y)[0].trim())
													&& subFondo.trim().equals(idSerie.get(y)[1].trim())
													&& subFondo2.trim().equals(idSerie.get(y)[2].trim())) {
												keySubFondo2 = soggettoConservatore + "." + fondo + "." + subFondo + "."
														+ subFondo2;
												if (subFondo2Scheda == null) {
													if (params == null) {
														params = Item.convert(doc);
													}
													params.add(ItemTeca.SUBFONDO2SCHEDA, keySubFondo2);
													nSubFondi2++;
												} else if (subFondo2Scheda.equals("")
														|| !subFondo2Scheda.equals(keySubFondo2)) {
													if (params == null) {
														params = Item.convert(doc);
													}
													params.getParams().remove(ItemTeca.SUBFONDO2SCHEDA);
													params.add(ItemTeca.SUBFONDO2SCHEDA, keySubFondo2);
													nSubFondi2++;
												}
											}
										}
										// }
									}
								}
							}
						}
						if (params != null) {
							admd.add(params.getParams(), new ItemTeca());
						}
					}
					System.out.println("nSoggettoCons[" + soggettoConservatore + "]: " + nSoggettoCons + "\tnFondi: "
							+ nFondi + "\tnSubFondi: " + nSubFondi + "\tnSubFondi2: " + nSubFondi2);
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
	}
}

class DatiIstituto {
	String descrizione = null;
	Vector<String> fondi = null;

	public DatiIstituto(String descrizione) {
		this.descrizione = descrizione;
	}

	/**
	 * @return the descrizione
	 */
	public String getDescrizione() {
		return descrizione;
	}

	public void addFondo(String fondo) {
		if (fondi == null) {
			fondi = new Vector<String>();
		}
		fondi.add(fondo);
	}

	/**
	 * @return the fondi
	 */
	public Vector<String> getFondi() {
		return fondi;
	}
}