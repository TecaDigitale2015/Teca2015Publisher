/**
 * 
 */
package mx.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import mx.randalf.solr.FindDocument;
import mx.randalf.solr.exception.SolrException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

/**
 * @author massi
 *
 */
public class analizzaDati {

	/**
	 * 
	 */
	public analizzaDati() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String url = null;
		boolean cloud = false;
		String collection = null;
		int connectionTimeout = 0;
		int clientTimeout = 0;
		String query = null;
		FindDocument find = null;
		QueryResponse qr = null;
		Vector<String> cols = null;
		SolrDocumentList response = null;
		BufferedWriter bw = null;
		FileWriter fw = null;
		File f = null;
		String fOutput = null;
		try {
			fOutput = "/Users/massi/temp/puglia/tmp/SchedeF_Finale14.csv";
//			fOutput = "/Users/massi/temp/puglia/tmp/TecaRic3_UfficioDistrettuale.csv";
//			fOutput = "/Users/massi/temp/puglia/tmp/TecaFinale_UfficioDistrettuale.csv";
			f = new File(fOutput);
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);
//			url = "http://sast-regionepuglia.it:8983/solr/tecaPuglia";
			url = "http://mxmuletto.bncf.lan:8983/solr/tecaProd3";
//			url = "http://mxmuletto.bncf.lan:8983/solr/tecaPuglia";
			cloud = false;
//			collection = "tecaPuglia";
			collection = "tecaProd3";
//			collection = "tecaPuglia";
			connectionTimeout= 60000;
			collection = "tecaProd3";
			clientTimeout= 100000;
			find = new FindDocument(url, cloud, collection, connectionTimeout, clientTimeout);

			query = "+tipologiaFile:SchedaF";
//			query = "+tipologiaFile:\"Uc\" "
//					+ "+soggettoConservatore:\"Archivio_di_Stato_di_Bari\" "
//					+ "+fondo:\"Comune_di_Bari\"";
//			query = "+tipologiaFile:\"Uc\" "
//					+ "+soggettoConservatore:\"Archivio_di_Stato_di_Bari\" "
//					+ "+fondo:\"Ufficio_distrettuale_delle_imposte_dirette\"";
			cols = new Vector<String>();
			cols.add("id");
			cols.add("bid_show");
			cols.add("titolo_show");
			cols.add("originalFileName_show");	
			qr = find.find(query,0,1000000, cols, null);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int y=0; y<cols.size(); y++){
						if (y>0){
							bw.write("|");
						}
						bw.write(cols.get(y));
					}
					bw.write("\n");
					for (int x=0; x<response.size(); x++){
						for (int y=0; y<cols.size(); y++){
							if (y>0){
								bw.write("|");
							}
							if (response.get(x).get(cols.get(y)) != null){
								if (response.get(x).get(cols.get(y)).getClass().getName().equals(ArrayList.class.getName())){
									ArrayList<Object> al = (ArrayList<Object>) response.get(x).get(cols.get(y));
									for (int z=0; z<al.size(); z++){
										if (z>0){
											bw.write("#");
										}
										bw.write(((String)al.get(z)).replace("\"", ""));
									}
								} else {
									bw.write((String)response.get(x).get(cols.get(y)));
								}
							}
						}
						bw.write("\n");
					}
				}
			}
		} catch (SolrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (bw != null){
					bw.flush();
					bw.close();
				}
				if (fw != null){
					fw.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
