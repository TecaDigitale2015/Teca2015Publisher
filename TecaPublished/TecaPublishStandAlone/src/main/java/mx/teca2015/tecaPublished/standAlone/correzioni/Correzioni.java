/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.correzioni;

import java.io.ByteArrayInputStream;
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
import mx.randalf.mag.MagNamespacePrefix;
import mx.randalf.schedaF.Scheda;
import mx.randalf.schedaF.SchedaXsd;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.Item;
import mx.randalf.solr.Params;
import mx.randalf.solr.exception.SolrException;
import mx.randalf.xsd.exception.XsdException;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class Correzioni {

	private static Logger log = Logger.getLogger(Correzioni.class);

	/**
	 * 
	 */
	public Correzioni() {
	}

	public static void aggND(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatore = null;
		String soggettoConservatoreKey = null;
		

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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ND\""
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
						soggettoConservatoreKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));
						soggettoConservatore = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATORE+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals("ND") && soggettoConservatore.equals("non identificabile")){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SOGGETTOCONSERVATORE);
									params.add(ItemTeca.SOGGETTOCONSERVATORE, "Archivio Adda");
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
					System.out.println("aggND: "+trovati);
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

	public static void aggAggFondo(IndexDocumentTeca admd, String idSoggettoConservatoreKey, 
			String idFondoKey, String fondoOld, String fondoNew) throws JobExecutionException{
		aggAggFondo(admd, idSoggettoConservatoreKey, idFondoKey, fondoOld, null, fondoNew);
	}

	public static void aggAggFondo(IndexDocumentTeca admd, String idSoggettoConservatoreKey, 
			String idFondoKey, String fondoOld, String idFondoKeyNew, String fondoNew) throws JobExecutionException{
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
		String fondo = null;
		
		
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\""+idSoggettoConservatoreKey+"\""
						+" +"+ItemTeca.FONDOKEY+":\""+idFondoKey+"\""
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
							fondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDO+"_show")).get(0));

							if (soggettoConservatoreKey.equals(idSoggettoConservatoreKey) && 
									fondoKey.equals(idFondoKey) && 
									fondo.equals(fondoOld) ){
								
									params = Item.convert(doc);
									if (idFondoKeyNew != null){
										params.getParams().remove(ItemTeca.FONDOKEY);
										params.add(ItemTeca.FONDOKEY, idFondoKeyNew);
									}
									params.getParams().remove(ItemTeca.FONDO);
									params.add(ItemTeca.FONDO, fondoNew);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
				}
				System.out.println("agg"+idSoggettoConservatoreKey+"_"+idFondoKey+": "+trovati);
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

	public static void aggAggSoggettoConservatore(IndexDocumentTeca admd, String idSoggettoConservatoreKey, 
			String soggettoConservatoreOld, String idSoggettoConservatoreKeyNew, String soggettoConservatoreNew) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
		String soggettoConservatore = null;
		
		
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\""+idSoggettoConservatoreKey+"\""
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
							soggettoConservatore = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATORE+"_show")).get(0));

							if (soggettoConservatoreKey.equals(idSoggettoConservatoreKey) && 
									soggettoConservatore.equals(soggettoConservatoreOld)){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SOGGETTOCONSERVATORE);
									params.add(ItemTeca.SOGGETTOCONSERVATORE, soggettoConservatoreNew);
									
									if (idSoggettoConservatoreKeyNew != null){
										params.getParams().remove(ItemTeca.SOGGETTOCONSERVATOREKEY);
										params.add(ItemTeca.SOGGETTOCONSERVATOREKEY, idSoggettoConservatoreKeyNew);
									}
									
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
				}
				System.out.println("agg"+idSoggettoConservatoreKey+": "+trovati);
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

	public static void aggAggSubFondo(IndexDocumentTeca admd, String idSoggettoConservatoreKey, 
			String idFondoKey, String idSubFondoKey, String subFondoOld, String subFondoNew) throws JobExecutionException{
		aggAggSubFondo(admd, idSoggettoConservatoreKey, idFondoKey, idSubFondoKey, subFondoOld, null, subFondoNew);
	}

	public static void aggAggSubFondo(IndexDocumentTeca admd, String idSoggettoConservatoreKey, 
			String idFondoKey, String idSubFondoKey, String subFondoOld, 
			String idSubFondoKeyNew, String subFondoNew) throws JobExecutionException{
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\""+idSoggettoConservatoreKey+"\""
						+" +"+ItemTeca.FONDOKEY+":\""+idFondoKey+"\""
						+" +"+ItemTeca.SUBFONDOKEY+":\""+idSubFondoKey+"\""
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
						soggettoConservatoreKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show")).get(0));
						fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
						subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));
						subFondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals(idSoggettoConservatoreKey) && 
									fondoKey.equals(idFondoKey) && 
									subFondoKey.equals(idSubFondoKey) && 
									subFondo.equals(subFondoOld) ){
								
									params = Item.convert(doc);
									if (idSubFondoKeyNew != null){
										params.getParams().remove(ItemTeca.SUBFONDOKEY);
										params.add(ItemTeca.SUBFONDOKEY, idSubFondoKeyNew);
									}
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.add(ItemTeca.SUBFONDO, subFondoNew);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
				}
				System.out.println("agg"+idSoggettoConservatoreKey+"_"+idFondoKey+"_"+idSubFondoKey+": "+trovati);
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

	public static void aggSubFondoScheda(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String subFondoScheda = null;
		
		
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
			query = "-"+ItemTeca.SUBFONDOSCHEDA+":[\"\" TO *] "+
							"("+ItemTeca.TIPOLOGIAFILE+":"+ItemTeca.TIPOLOGIAFILE_UC+" "+
								ItemTeca.TIPOLOGIAFILE+":"+ItemTeca.TIPOLOGIAFILE_UD+" "+
								ItemTeca.TIPOLOGIAFILE+":"+ItemTeca.TIPOLOGIAFILE_SCHEDAF+")"
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
						subFondoScheda = (String) doc.getFirstValue(ItemTeca.SUBFONDOSCHEDA+"_show");
						
						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){
							if (subFondoScheda == null){
								params = Item.convert(doc);
								params.add(ItemTeca.SUBFONDOSCHEDA, "No");
								admd.add(params.getParams(), new ItemTeca());
								trovati++;
							}
						}
					}
				}
				System.out.println("aggSubFondoScheda: "+trovati);
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

	public static void allinea(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;

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
			query = ItemTeca.TIPOLOGIAFILE+":\""+ItemTeca.TIPOLOGIAFILE_SOGGETTOCONSERVATORE+"\"";
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						System.out.println((String) doc.getFirstValue(ItemTeca.BID+"_show")+
								" - "+
								(String) doc.getFirstValue(ItemTeca.TITOLO+"_show")+
								" = "+
								allinea(admd, (String) doc.getFirstValue(ItemTeca.BID+"_show"), (String) doc.getFirstValue(ItemTeca.TITOLO+"_show")));
					}
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

	private static int allinea(IndexDocumentTeca admd, String soggettoConservatoreKey, String soggettoConservatore) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		int result = 0;
		String tipologiaFile = null;
		String sogConservatoreKey = null;
		String sogConservatore = null;
		Params params = null;
		SchedaXsd schedafXsd = null;
		Scheda schedaf = null;

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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\""+soggettoConservatoreKey+"\"";
			qr = find.find(query,0,1000000);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					schedafXsd = new SchedaXsd();
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						tipologiaFile = (String) doc.getFirstValue(ItemTeca.TIPOLOGIAFILE+"_show");
						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF)){
							sogConservatoreKey = (String) doc.getFirstValue(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show");
							sogConservatore = (String) doc.getFirstValue(ItemTeca.SOGGETTOCONSERVATORE+"_show");
							params = null;
							if (sogConservatoreKey.equals(soggettoConservatoreKey) && !sogConservatore.equals(soggettoConservatore)){
								params = Item.convert(doc);
								params.getParams().remove(ItemTeca.SOGGETTOCONSERVATORE);
								params.add(ItemTeca.SOGGETTOCONSERVATORE, soggettoConservatore);
							}
							if (sogConservatoreKey.equals(soggettoConservatoreKey) && tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF)){
								schedaf = schedafXsd.read(new ByteArrayInputStream(((String) doc.getFirstValue(ItemTeca.XMLSCHEDAF)).getBytes()));

								if (!schedaf.getLC().getLDC().getLDCN().getValue().equals(soggettoConservatore)){
									schedaf.getLC().getLDC().getLDCN().setValue(soggettoConservatore);
									if (params== null){
										params = Item.convert(doc);
									}
									params.getParams().remove(ItemTeca.XMLSCHEDAF);
									params.add(ItemTeca.XMLSCHEDAF, schedafXsd.write(schedaf, new MagNamespacePrefix(), null, null, null));
//									System.out.println(schedaf.getLC().getLDC().getLDCN().getValue());
								}
								
							}
							if (params != null){
								admd.add(params.getParams(), new ItemTeca());
								result++;
							}
						}
					}
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
		} catch (XsdException e) {
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
		return result;
	}
}
