/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

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
import mx.teca2015.tecaPublished.standAlone.correzioni.Correzioni;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniACO;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniAQP;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniASBA;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniASBR;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniASFG;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniASFG_LU;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniBAD;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniBCLU;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniBNBA;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniBPF;
import mx.teca2015.tecaPublished.standAlone.correzioni.CorrezioniND;
import mx.teca2015.tecaUtility.solr.IndexDocumentTeca;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class TecaPublisherCorreggi {

	private static Logger log = Logger.getLogger(TecaPublishDatiPerSan.class);
	
	/**
	 * @throws ConfigurationException 
	 * 
	 */
	public TecaPublisherCorreggi(String fileProp) throws ConfigurationException {
		File f = null;
		f = new File(fileProp);
		Configuration.init((f.getParentFile()==null?"./":f.getParentFile().getAbsolutePath()));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TecaPublisherCorreggi publish = null;

		try {
			if (args.length == 1) {
				publish = new TecaPublisherCorreggi(args[0]);
//			System.out.println("Inizio Pubblicazione");
				log.info("Inizio pubblicazione");
				publish.correggi();
			} else {
				System.out
						.println("E' ncessario indicare il file di configurazione");
			}
		} catch (ConfigurationException e) {
			log.error(e.getMessage(), e);
		}

	}

	public void correggi(){
		IndexDocumentTeca admd = null;
		File fSolr = null;

		try {
			admd = new IndexDocumentTeca();
			
//			aggASBA_CRSCC(admd);
//			aggASBA_AD(admd);
//			aggND(admd);
//			aggASBR_CR(admd);
//			aggASBR_CAOS(admd);
//			aggASBR_MSBO(admd);
//			aggASBR_CSDC(admd);
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLII-TITII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLII-TITIII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLIII-TITIII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLII-TITVII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLIII-TITIV");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLIII-TITVI");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLIII-TITVII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLV-TITII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLV-TITIII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLV-TITVII");
//			aggASBR_GCBR_IVER_CLII(admd, "IVER-CLV-TITIV");
////			aggASBR_GCBR_IVER_CLII_TITII(admd);
//			aggAggFondo(admd, "ASFG", "DPP", "Dogana delle pecore di Puglia", "Dogana delle pecore di Foggia");
//			aggAggFondo(admd, "ASFG", "CTA", "Catasti antichi e catasti provvisori", "Catasto provvisorio");
//			aggAggFondo(admd, "ASFG", "PRL", "Progetti di lavoro", "Progetti di lavori");
//			aggAggSubFondo(admd, "ASBR", "GCBR", "IIVER", "II Versamento", "Genio civile - 2° versamento");
//			aggSubFondoScheda(admd);
//			aggASBA_ITBA_CAMP(admd);
//			CorrezioniASBR.aggASBR_GCBR_IVER(admd);
//			Correzioni.aggAggFondo(admd, "ASFG", "DRRT", "Direzione di reintegra dei regi tratturi", "Reintegra dei tratturi, comprende anche Direzione del servizio di custodia e degli affitti dei tratturi di Foggia 1878-1914");
//			Correzioni.aggAggFondo(admd, "ASBR", "GCBR", "Genio civile di Brindisi", "Ufficio del genio Civile di Brindisi");
//			Correzioni.aggAggFondo(admd, "ASLE", "GCL", "Genio civile di Lecce", "Ufficio del Genio Civile di Lecce nella Guida");
//			Correzioni.aggAggFondo(admd, "ASLE", "GCV", "Gran Corte della Vicaria di Napoi", "Gran Corte della Vicaria (di Napoi)");
//			CorrezioniASBA_TRANI.aggTRANI_GCCT_PE(admd);
//			Correzioni.aggAggSoggettoConservatore(admd, "ASBA-TRANI", "Archivio di Stato di Bari. Sezione di Trani", "Sezione di Archivio di Stato di Trani");
//			CorrezioniALSE.aggASLE_PRTO_II_LLPP(admd);
			CorrezioniASBA.esegui(admd);
			CorrezioniASFG.esegui(admd);
			CorrezioniASFG_LU.esegui(admd);
			CorrezioniACO.esegui(admd);
			CorrezioniBCLU.esegui(admd);
			CorrezioniASBR.esegui(admd);
			CorrezioniBNBA.esegui(admd);
			CorrezioniBAD.esegui(admd);
			CorrezioniND.esegui(admd);
			CorrezioniBPF.esegui(admd);
			CorrezioniAQP.esegui(admd);

			Correzioni.allinea(admd);
			
			fSolr = new File("./correzioni.solr");
			if (fSolr.exists()){
				fSolr.delete();
			}
			admd.write(fSolr);
			if (fSolr.exists()){
				admd.publish(Configuration.getValue("solr.batchPost"), fSolr);
				admd.optimize();
			}

//			admd.clear();
//			clean(admd);
//			fSolr = new File("./correzioni.solr");
//			if (fSolr.exists()){
//				fSolr.delete();
//			}
//			admd.write(fSolr);
//			if (fSolr.exists()){
//				admd.publish(Configuration.getValue("solr.batchPost"), fSolr);
//				admd.optimize();
//			}
		} catch (JobExecutionException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} catch (SolrException e) {
			log.error(e.getMessage(), e);
		} catch (ConfigurationException e) {
			log.error(e.getMessage(), e);
		}

	}

	 void clean(IndexDocumentTeca admd) throws JobExecutionException{
		FindDocument find = null;
		String query = "";
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SolrDocument doc = null;
		Params params = null;
		int trovati =0;
		String tipologiaFile = null;

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
			query = "*:*"
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

							params = checkDuplicati(doc, ItemTeca.SOGGETTOCONSERVATORE, params);
							params = checkDuplicati(doc, ItemTeca.SOGGETTOCONSERVATOREKEY, params);
							params = checkDuplicati(doc, ItemTeca.SOGGETTOCONSERVATORESCHEDA, params);
							params = checkDuplicati(doc, ItemTeca.FONDO, params);
							params = checkDuplicati(doc, ItemTeca.FONDOKEY, params);
							params = checkDuplicati(doc, ItemTeca.FONDOSCHEDA, params);
							params = checkDuplicati(doc, ItemTeca.SUBFONDO, params);
							params = checkDuplicati(doc, ItemTeca.SUBFONDOKEY, params);
							params = checkDuplicati(doc, ItemTeca.SUBFONDOSCHEDA, params);
							params = checkDuplicati(doc, ItemTeca.SUBFONDO2, params);
							params = checkDuplicati(doc, ItemTeca.SUBFONDO2KEY, params);
							params = checkDuplicati(doc, ItemTeca.SUBFONDO2SCHEDA, params);
							
							if (params != null){
								admd.add(params.getParams(), new ItemTeca());
								trovati++;
							}
						}
					}
					System.out.println("clean: "+trovati);
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

	private Params checkDuplicati(SolrDocument doc, String key, Params params){
		Vector<String> dati = null;
		ArrayList<Object> values = null;
		if (doc.getFirstValue(key+"_show") != null){
			dati = new Vector<String>();
			try {
			values = (ArrayList<Object>) doc.getFieldValues(key+"_show");
			for (int y=0; y<values.size(); y++){
				if (!dati.contains(values.get(y))){
					dati.add((String) values.get(y));
				}
			}
			if (values.size()!=dati.size()){
				System.out.println(key+" -> "+values.size()+" = "+dati.size());
				if (params==null){
					params = Item.convert(doc);
				}
				params.getParams().remove(key);
				for (int x=0; x<dati.size(); x++){
					params.add(key, dati.get(x));
				}
			}
			} catch (ClassCastException e){
				e.printStackTrace();
			}
		}
		return params;
	}
}
