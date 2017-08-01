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
public class CorrezioniASFG {

	private static Logger log = Logger.getLogger(CorrezioniASFG.class);

	/**
	 * 
	 */
	public CorrezioniASFG() {
	}

	public static void esegui(IndexDocumentTeca admd) throws JobExecutionException{
//		aggASFG_DPP_GEN(admd);
//		Correzioni.aggAggFondo(admd, "ASFG", "CTA", "Catasto provvisorio", "Catasti antichi e provvisori");
//		aggASFG_CTA_CP(admd);
//		Correzioni.aggAggFondo(admd, "ASFG", "IPC", "Intendenza e Prefettura di Capitanata", "Intendenza, Governo e Prefettura  di Capitanata");
//		Correzioni.aggAggSubFondo(admd, "ASFG", "IPC", "AC", "Affari comunali", "AC2", "Affari comunali, serie II e Appendice I");
//		Correzioni.aggAggSubFondo(admd, "ASFG", "IPC", "AD", "Affari demaniali", "AD2", "Affari demaniali");
//		aggASFG_IPC_SCP(admd);
//		aggASFG_TCC(admd);
//		aggASFG_PRL_PROG(admd);
//		aggASFG_PT_PTOP(admd);
	}
	
	static  void aggASFG_DPP_GEN(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG\""
					+" +"+ItemTeca.FONDOKEY+":\"DPP\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"GEN\""
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

							if (soggettoConservatore.equals("ASFG")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));

								if (fondoKey.equals("DPP") 
										&& subFondoKey.equals("GEN")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.add(ItemTeca.SUBFONDOKEY, "GEN2");
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
					System.out.println("aggASFG_DPP_GEN: "+trovati);
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
	
	static  void aggASFG_CTA_CP(IndexDocumentTeca admd) throws JobExecutionException{
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
//		String subFondoKey = null;
		String subFondo = null;
		

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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG\""
					+" +"+ItemTeca.FONDOKEY+":\"CTA\""
					+" +"+ItemTeca.SUBFONDO+":\"Catasti provvisori\""
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

							if (soggettoConservatore.equals("ASFG")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
//								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("CTA") 
										&& subFondo.equals("Catasti provvisori")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
				}
				System.out.println("aggASFG_CTA_CP: "+trovati);
				
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
	
	static  void aggASFG_IPC_SCP(IndexDocumentTeca admd) throws JobExecutionException{
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
		String subFondoKey = null;
//		String subFondo = null;
		

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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG\""
					+" +"+ItemTeca.FONDOKEY+":\"IPC\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"SCP\""
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

							if (soggettoConservatore.equals("ASFG")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("IPC") 
										&& subFondoKey.equals("SCP")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.FONDO);

									params.add(ItemTeca.FONDOKEY, "CIC");
									params.add(ItemTeca.FONDO, "Consiglio di Intendenza di Capitanata");
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
				}
				System.out.println("aggASFG_CTA_CP: "+trovati);
				
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
	
	static  void aggASFG_TCC(IndexDocumentTeca admd) throws JobExecutionException{
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
//		String subFondoKey = null;
//		String subFondo = null;
		

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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG\""
					+" +"+ItemTeca.FONDOKEY+":\"TCC\""
//					+" +"+ItemTeca.SUBFONDOKEY+":\"SCP\""
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

							if (soggettoConservatore.equals("ASFG")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
//								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("TCC") 
//										&& subFondoKey.equals("SCP")
										){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SOGGETTOCONSERVATOREKEY);
									params.getParams().remove(ItemTeca.SOGGETTOCONSERVATORE);

									params.add(ItemTeca.SOGGETTOCONSERVATOREKEY, "ASFG-LU");
									params.add(ItemTeca.SOGGETTOCONSERVATORE, "Archivio di Stato di Foggia. Sezione di Lucera");
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
				}
				System.out.println("aggASFG_CTA_CP: "+trovati);
				
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
	
	static  void aggASFG_PRL_PROG(IndexDocumentTeca admd) throws JobExecutionException{
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
		String subFondoKey = null;
//		String subFondo = null;
		

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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG\""
					+" +"+ItemTeca.FONDOKEY+":\"PRL\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"PROG\""
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

							if (soggettoConservatore.equals("ASFG")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("PRL") 
										&& subFondoKey.equals("PROG")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
				}
				System.out.println("aggASFG_CTA_CP: "+trovati);
				
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
	
	static  void aggASFG_PT_PTOP(IndexDocumentTeca admd) throws JobExecutionException{
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
		String subFondoKey = null;
//		String subFondo = null;
		

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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG\""
					+" +"+ItemTeca.FONDOKEY+":\"PT\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"PTOP\""
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

							if (soggettoConservatore.equals("ASFG")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("PT") 
										&& subFondoKey.equals("PTOP")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
								}
							}
						}
					}
				}
				System.out.println("aggASFG_CTA_CP: "+trovati);
				
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