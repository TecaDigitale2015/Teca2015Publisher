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
public class CorrezioniASBR {

	private static Logger log = Logger.getLogger(CorrezioniASBR.class);

	/**
	 * 
	 */
	public CorrezioniASBR() {
	}

	public static void esegui(IndexDocumentTeca admd) throws JobExecutionException{
//		aggASBR_CR_PL(admd);
//		Correzioni.aggAggSubFondo(admd, "ASBR", "CR", "CAOS", "Capitolo di Ostuni", "CAOS2", "Capitolo di Ostuni");
//		aggASBR_GCBR_AES(admd);
//		aggASBR_AN_AES(admd);
//		Correzioni.aggAggFondo(admd, "ASBR", "AN", "Atti notarili", "Atti dei Notai");
//		aggASBR_GCBR_IIVER(admd);
	}
	
	static void aggASBR_CR(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"CR\""
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

							if (fondoKey.equals("CR") && fondo.equals("Corporazioni religiose")){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDO);
									params.add(ItemTeca.FONDO, "Corporazioni religiose di Brindisi e comuni della provincia");
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
					System.out.println("aggASBR_CR: "+trovati);
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

	static void aggASBR_CAOS(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"CAOS\""
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
						fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
						fondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDO+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals("ASBR") && fondoKey.equals("CAOS")){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDO);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.add(ItemTeca.FONDO, "Corporazioni religiose di Brindisi e comuni della provincia");
									params.add(ItemTeca.FONDOKEY, "CR");
									params.add(ItemTeca.SUBFONDO, fondo);
									params.add(ItemTeca.SUBFONDOKEY, fondoKey);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
					System.out.println("aggASBR_CAOS: "+trovati);
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

	static void aggASBR_MSBO(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"MSBO\""
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
						fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
						fondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDO+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals("ASBR") && fondoKey.equals("MSBO")){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDO);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.add(ItemTeca.FONDO, "Corporazioni religiose di Brindisi e comuni della provincia");
									params.add(ItemTeca.FONDOKEY, "CR");
									params.add(ItemTeca.SUBFONDO, fondo);
									params.add(ItemTeca.SUBFONDOKEY, fondoKey);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
					System.out.println("aggASBR_MSBO: "+trovati);
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

	static void aggASBR_CSDC(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;
		String soggettoConservatoreKey = null;
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"CSDC\""
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
						fondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDO+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals("ASBR") && fondoKey.equals("CSDC")){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.FONDO);
									params.getParams().remove(ItemTeca.FONDOKEY);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.add(ItemTeca.FONDO, "Corporazioni religiose di Brindisi e comuni della provincia");
									params.add(ItemTeca.FONDOKEY, "CR");
									params.add(ItemTeca.SUBFONDO, fondo);
									params.add(ItemTeca.SUBFONDOKEY, fondoKey);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
					System.out.println("aggASBR_CSDC: "+trovati);
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

	static void aggASBR_GCBR_IVER_CLII(IndexDocumentTeca admd, String idSubFondo) throws JobExecutionException{
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"GCBR\""
						+" +"+ItemTeca.SUBFONDOKEY+":\""+idSubFondo+"\""
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
						subFondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0));
						subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals("ASBR") && 
									fondoKey.equals("GCBR") && 
									subFondoKey.equals(idSubFondo) ){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDO);
									params.getParams().remove(ItemTeca.SUBFONDOKEY);
									params.add(ItemTeca.SUBFONDO, "Genio civile - 1° versamento");
									params.add(ItemTeca.SUBFONDOKEY, "IVER");
									params.add(ItemTeca.SUBFONDO2, subFondo.replace("I Versamento-", ""));
									params.add(ItemTeca.SUBFONDO2KEY, subFondoKey);
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
				}
				System.out.println("aggASBR_GCBR_"+idSubFondo+": "+trovati);
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

	static void aggASBR_GCBR_IVER(IndexDocumentTeca admd) throws JobExecutionException{
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
		String subFondo2Key = null;
		

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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"GCBR\""
						+" +"+ItemTeca.SUBFONDOKEY+":\"IVER\""
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

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){
							subFondo2Key = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO2KEY+"_show")).get(0));

							if (soggettoConservatoreKey.equals("ASBR") && 
									fondoKey.equals("GCBR") && 
									subFondoKey.equals("IVER") && 
									subFondo2Key.startsWith("IVER-") ){
								
									params = Item.convert(doc);
									params.getParams().remove(ItemTeca.SUBFONDO2KEY);
									params.add(ItemTeca.SUBFONDO2KEY, subFondo2Key.substring(5));
									admd.add(params.getParams(), new ItemTeca());
									trovati++;
							}
						}
					}
				}
				System.out.println("aggASBR_GCBR_IVER: "+trovati);
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

	static void aggASBR_CBR_ACBR(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
						+" +"+ItemTeca.FONDOKEY+":\"CBR\""
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

						if (tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_SCHEDAF) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UC) ||
								tipologiaFile.equals(ItemTeca.TIPOLOGIAFILE_UD) ){

							if (soggettoConservatoreKey.equals("ASBR") && fondoKey.equals("CBR")){
								if (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")!= null){
									subFondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0));
									if (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null){
										if (subFondoKey.trim().equals("ACBR")){
											params = Item.convert(doc);
											params.getParams().remove(ItemTeca.SUBFONDO);
											params.getParams().remove(ItemTeca.SUBFONDOKEY);
											params.add(ItemTeca.SUBFONDO, "Archivio storico del comune di Brindisi");
											params.add(ItemTeca.SUBFONDOKEY, "ACBR");
											admd.add(params.getParams(), new ItemTeca());
											trovati++;
										}
									} else {
										subFondo = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0));
										if (subFondoKey.trim().startsWith("ACBR-") || subFondoKey.trim().startsWith("ASBR-")){
											params = Item.convert(doc);
											params.getParams().remove(ItemTeca.SUBFONDO);
											params.getParams().remove(ItemTeca.SUBFONDOKEY);
											params.add(ItemTeca.SUBFONDO, "Archivio storico del comune di Brindisi");
											params.add(ItemTeca.SUBFONDOKEY, "ACBR");
											params.add(ItemTeca.SUBFONDO2, subFondo);
											params.add(ItemTeca.SUBFONDO2KEY, subFondoKey.trim().substring(5));
											admd.add(params.getParams(), new ItemTeca());
											trovati++;
										}
									}
								}
							}
						}
					}
					System.out.println("aggASBR_CSDC: "+trovati);
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
	
	static  void aggASBR_CR_PL(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
					+" +"+ItemTeca.FONDOKEY+":\"CR\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"PL\""
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

							if (soggettoConservatore.equals("ASBR")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("CR") 
										&& subFondoKey.equals("PL")){
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
	
	static  void aggASBR_GCBR_AES(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
					+" +"+ItemTeca.FONDOKEY+":\"GCBR\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"AES\""
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

							if (soggettoConservatore.equals("ASBR")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("GCBR") 
										&& subFondoKey.equals("AES")){
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
	
	static  void aggASBR_AN_AES(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
					+" +"+ItemTeca.FONDOKEY+":\"AN\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"AES\""
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

							if (soggettoConservatore.equals("ASBR")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("AN") 
										&& subFondoKey.equals("AES")){
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
	
	static  void aggASBR_GCBR_IIVER(IndexDocumentTeca admd) throws JobExecutionException{
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
			query = "+"+ItemTeca.SOGGETTOCONSERVATOREKEY+":\"ASBR\""
					+" +"+ItemTeca.FONDOKEY+":\"GCBR\""
					+" +"+ItemTeca.SUBFONDOKEY+":\"IIVER\""
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

							if (soggettoConservatore.equals("ASBR")){
								
								fondoKey = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.FONDOKEY+"_show")).get(0));
								subFondoKey = (doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDOKEY+"_show")).get(0)));
//								subFondo = (doc.getFieldValues(ItemTeca.SUBFONDO+"_show") == null?null:((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.SUBFONDO+"_show")).get(0)));

								if (fondoKey.equals("GCBR") 
										&& subFondoKey.equals("IIVER")){
									params = Item.convert(doc);

									params.add(ItemTeca.SUBFONDO2, "II Versamento");
									params.add(ItemTeca.SUBFONDO2KEY, "IIVER2");
									
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
