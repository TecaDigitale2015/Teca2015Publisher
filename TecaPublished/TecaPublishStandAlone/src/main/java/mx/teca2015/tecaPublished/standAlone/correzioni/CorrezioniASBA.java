/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.correzioni;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.quartz.JobExecutionException;

import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.Item;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class CorrezioniASBA {

	private static Logger log = Logger.getLogger(CorrezioniASBA.class);

	/**
	 * 
	 */
	public CorrezioniASBA() {
	}

	public static void esegui(IndexDocumentTeca admd) throws JobExecutionException{
//		Correzioni.aggAggSubFondo(admd, "ASBA", "UDID", "ACF", "Atti catastali dei fabbricati", "Atti catastali dei fabbricati - Catasto Fabbricati");
//		aggASBA_UDID_ACF(admd);
//		aggASBA_PBA(admd);
//		Correzioni.aggAggSubFondo(admd, "ASBA", "PBA", "GAB-III", "Gabinetto III versamento", "Gabinetto");
//		Correzioni.aggAggFondo(admd, "ASBA", "COMBA", "Comune di Bari", "Comune di Bari e frazioni: Carbonara, Ceglie, Loseto, Palese, S. Spirito, Torre a Mare");
//		Correzioni.aggAggFondo(admd, "ASBA", "TDB", "Tribunale di Bari", "Tribunale Civile di Bari");
//		aggASBA_TDB_PE(admd);
//		aggASBA_UDID_ACF2(admd);
	}
	
	static  void aggASBA_ITBA_CAMP(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatore = null;
		String fondoKey = null;
		String subFondo = null;
		String subFondoKey = null;
		

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBA\""
					+" +"+ItemTeca.FONDOKEY+":\"ITBA\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"CAMP\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));
						soggettoConservatore = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatore.equals("ASBA")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("ITBA") && subFondoKey.equals("CAMP")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.FONDO);
									params.add(ItemTeca.FONDOKEY, "PBA");
									params.add(ItemTeca.FONDO, "Prefettura di Bari");
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.add(ItemTeca.SUBFONDOKEY, "AG");
									params.add(ItemTeca.SUBFONDO, "Archivio generale");
									params.add(ItemTeca.SUBFONDOSCHEDA, "No");
									params.add(ItemTeca.SUBFONDO2KEY, subFondoKey);
									params.add(ItemTeca.SUBFONDO2, subFondo);
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
					System.out.println("aggASBA_CRSCC: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

	static void aggASBA_CRSCC(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatore = null;
		String fondo = null;
		String fondoKey = null;
		String subFondo = null;
		String subFondoKey = null;
		

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBA\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));
						soggettoConservatore = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatore.equals("ASBA")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								fondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDO+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (subFondoKey != null && subFondoKey.equals("CRSCC")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.FONDO);
									params.add(ItemTeca.FONDOKEY, subFondoKey);
									params.add(ItemTeca.FONDO, subFondo);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.add(ItemTeca.SUBFONDOKEY, fondoKey);
									params.add(ItemTeca.SUBFONDO, fondo);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
					System.out.println("aggASBA_CRSCC: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

	static void aggASBA_AD(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String fondo = null;
		String fondoKey = null;
		

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = ItemTeca.FONDOKEY+"_fc:\"AD\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));
						fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
						fondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDO+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (fondoKey.equals("AD") && fondo.equals("Affari demaniali")){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDO);
									params.add(ItemTeca.FONDO, "Atti demaniali");
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
					System.out.println("aggASBA_AD: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

	static void aggASBA_UDID_ACF(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
		String fondoKey = null;
		String subFondoKey = null;
		String bid = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBA\""
					+"+"+ItemTeca.FONDOKEY+":\"UDID\""
					+"+"+ItemTeca.SUBFONDOKEY+":\"ACF\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							soggettoConservatoreKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));
							fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
							subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));
							bid = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.BID+"_show")).get(0));
							
							if (soggettoConservatoreKey.equals("ASBA") 
									&& fondoKey.equals("UDID") 
									&& subFondoKey.equals("ACF") 
									){
								if (doc.getFieldValues(ItemTeca.SUBFONDO2KEY+"_show")==null 
										){

									if (bid.startsWith("ASBA_UDID_ACF_CARTALBEROBELLO_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTPUTIGNANO_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTTURI_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTNOCI_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTLOCOROTONDO_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTBEROBELLO");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Putignano - Alberobello Locorotondo - "
												+ "Noci - Turi");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTPALO DEL COLLE_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTGIOVINAZZO_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTPALO﻿");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Bitonto - Giovinazzo - Palo");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTRUVO_DI_PUGLIA_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTTERLIZZI_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTRUVO DI PUGLIA_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTRUVO﻿");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Terlizzi - Ruvo");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTSAMMICHELE_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTSANNICANDRO_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTSAMMICHELE");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Adelfia - Canneto - Montrone - "
												+ "Casamassima - Sammichele - Sannicandro - Valenzano");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTPOLIGNANO_")){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTPOLIGNANO﻿");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Monopoli - Polignano");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTALTAMURA_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTGRAVINA_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTALTAMURA");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Altamura - Gravina - Poggiorsini");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTCONVERSANO_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTNOICATTARO_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTCASTELLANA_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTCONVERSANO");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Conversano - Castellana - "
												+ "Noicattaro - Rutigliano");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTTORITTO_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTCAPURSO_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTCARBONARA_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTCEGLIE_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTCELLAMARE_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTGRUMO_")
											){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTCAPURSO");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Bari - Carbonara - Loseto - "
												+ "S.Spirito - Ceglie - Torre a Mare - Binetto - Bitetto - Bitritto - Capurso - Cellamare - Grumo - Mola - Triggiano - Toritto");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTPALESE_")){
										params = Item.convert(doc);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTPALESE");
										params.add(ItemTeca.SUBFONDO2, "Catasto fabbricati dell'Ufficio distrettuale delle imposte dirette di Gioia - Acquaviva - Cassano - Santeramo");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									}
								}
							}
						}
					}
					System.out.println("aggASBA_AD: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

	static void aggASBA_UDID_ACF2(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
		String fondoKey = null;
		String subFondoKey = null;
		String bid = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBA\""
					+"+"+ItemTeca.FONDOKEY+":\"UDID\""
					+"+"+ItemTeca.SUBFONDOKEY+":\"ACF\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							soggettoConservatoreKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));
							fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
							subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));
							bid = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.BID+"_show")).get(0));
							
							if (soggettoConservatoreKey.equals("ASBA") 
									&& fondoKey.equals("UDID") 
									&& subFondoKey.equals("ACF") 
									){
								if (bid.startsWith("ASBA_UDID_ACF_CARTPALO DEL COLLE_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTGIOVINAZZO_")
											){
										params = Item.convert(doc);
										params.getParams().remove(ItemTeca.SUBFONDO2KEY);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTPALO");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTRUVO_DI_PUGLIA_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTTERLIZZI_")
											|| bid.startsWith("ASBA_UDID_ACF_CARTRUVO DI PUGLIA_")
											){
										params = Item.convert(doc);
										params.getParams().remove(ItemTeca.SUBFONDO2KEY);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTRUVO");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									} else if (bid.startsWith("ASBA_UDID_ACF_CARTPOLIGNANO_")){
										params = Item.convert(doc);
										params.getParams().remove(ItemTeca.SUBFONDO2KEY);
										params.add(ItemTeca.SUBFONDO2KEY, "CARTPOLIGNANO");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
									}
							}
						}
					}
					System.out.println("aggASBA_AD: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

	static void aggASBA_PBA(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
		String fondoKey = null;
		String subFondoKey = null;
//		String bid = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBA\""
					+"+"+ItemTeca.FONDOKEY+":\"PBA\""
//					+"+"+ItemTeca.SUBFONDOKEY+":\"ACF\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							soggettoConservatoreKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));
							fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
							subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));
//							bid = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.BID+"_show")).get(0));

							if (soggettoConservatoreKey.equals("ASBA") 
									&& fondoKey.equals("PBA") 
									){
								if (subFondoKey.equals("APSRP")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.add(ItemTeca.SUBFONDOKEY, "AG");
									params.add(ItemTeca.SUBFONDO, "Archivio Generale");
									params.add(ItemTeca.SUBFONDO2KEY, "APSRP");
									params.add(ItemTeca.SUBFONDO2, "Strade regie e provinciali");
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								} else if (subFondoKey.equals("SC")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.add(ItemTeca.SUBFONDOKEY, "AG");
									params.add(ItemTeca.SUBFONDO, "Archivio Generale");
									params.add(ItemTeca.SUBFONDO2KEY, "SC");
									params.add(ItemTeca.SUBFONDO2, "Strade comunali");
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								} else if (subFondoKey.equals("I-SE")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.add(ItemTeca.SUBFONDOKEY, "AG");
									params.add(ItemTeca.SUBFONDO, "Archivio Generale");
									params.add(ItemTeca.SUBFONDO2KEY, "I-SE");
									params.add(ItemTeca.SUBFONDO2, "Affari generali I serie");
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
					System.out.println("aggASBA_AD: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

	static void aggASBA_TDB_PE(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
		String fondoKey = null;
		String subFondoKey = null;
//		String bid = null;

		try {
			find = new FindDocument(Configuration.getValue("solr.URL"),
					Boolean.parseBoolean(Configuration
							.getValue("solr.Cloud")),
					Configuration
							.getValue("solr.collection"),
					Integer.parseInt(Configuration
							.getValue("solr.connectionTimeOut")),
					Integer.parseInt(Configuration
							.getValue("solr.clientTimeOut")));
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBA\""
					+"+"+ItemTeca.FONDOKEY+":\"TDB\""
					+"+"+ItemTeca.SUBFONDOKEY+":\"PE\""
//							+" -"+ItemTeca.SOGGETTOCONSERVATORESCHEDA+":Si"
							;
			
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						params = null;
						tipologiaFile = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.TIPOLOGIAFILE+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							soggettoConservatoreKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));
							fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
							subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));
//							bid = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.BID+"_show")).get(0));
							
							if (soggettoConservatoreKey.equals("ASBA") 
									&& fondoKey.equals("TDB") 
									&& subFondoKey.equals("PE") 
									){
										params = Item.convert(doc);
										params.getParams().remove(ItemTeca.SUBFONDOKEY);
										params.add(ItemTeca.SUBFONDOKEY, "PE2");
										admd.add(params.getParams(), new ItemTeca());
										trovati++;
							}
						}
					}
					System.out.println("aggASBA_AD: "+trovati);
				}
				
			}
		} catch (NumberFormatException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			log.error(e.getMessage(),e);
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				log.error(e.getMessage(),e);
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
	}

}
