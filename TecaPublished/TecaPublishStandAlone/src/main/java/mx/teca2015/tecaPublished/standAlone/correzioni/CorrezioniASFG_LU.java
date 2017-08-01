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
public class CorrezioniASFG_LU {

	private static Logger log = Logger.getLogger(CorrezioniASFG_LU.class);

	/**
	 * 
	 */
	public CorrezioniASFG_LU() {
	}

	public static void esegui(IndexDocumentTeca admd) throws JobExecutionException{
//		Correzioni.aggAggSubFondo(admd, "ASFG-LU", "TCC", "PE", "Perizie", "PE3", "Perizie");
//		Correzioni.aggAggSubFondo(admd, "ASFG-LU", "TCC", "ESPR", "Espropriazioni", "ESPR2", "Espropriazioni");
//		Correzioni.aggAggFondo(admd, "ASFG-LU", "CLU", "Città di Lucera", "Catasto di Lucera");
//		aggASFGLU_CLU_PE(admd);
//		aggASFGLU_CLU_SEII(admd);
	}
	
	static  void aggASFGLU_CLU_PE(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG-LU\""
					+" +"+ItemTeca.FONDOKEY+":\"CLU\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"PE\""
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

							if (soggettoConservatore.equals("ASFG-LU")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("CLU") 
										&& subFondoKey.equals("PE")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.FONDO);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);

									params.add(ItemTeca.FONDOKEY, "TCC");
									params.add(ItemTeca.FONDO, "Tribunale civile di Capitanata");

									params.add(ItemTeca.SUBFONDOKEY, "PE3");
									
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
	
	static  void aggASFGLU_CLU_SEII(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASFG-LU\""
					+" +"+ItemTeca.FONDOKEY+":\"CLU\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"SEII\""
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

							if (soggettoConservatore.equals("ASFG-LU")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("CLU") 
										&& subFondoKey.equals("SEII")){
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.FONDO);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);

									params.add(ItemTeca.FONDOKEY, "TCC");
									params.add(ItemTeca.FONDO, "Tribunale civile di Capitanata");

									params.add(ItemTeca.SUBFONDOKEY, "PE3");
									params.add(ItemTeca.SUBFONDO, "Perizie");
									
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