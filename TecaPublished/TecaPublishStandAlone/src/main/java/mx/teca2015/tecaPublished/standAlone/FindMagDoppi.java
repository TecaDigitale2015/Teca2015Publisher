/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.quartz.JobExecutionException;

import mx.randalf.solr.FindDocument;
import mx.randalf.solr.exception.SolrException;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class FindMagDoppi {

	public static Logger log = Logger.getLogger(FindMagDoppi.class);

	/**
	 * 
	 */
	public FindMagDoppi() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FindMagDoppi findMagDoppi = null;
		
		try {
			findMagDoppi = new FindMagDoppi();
			findMagDoppi.esegui();
		} catch (JobExecutionException e) {
			log.error(e.getMessage(), e);
		}

	}

	public void esegui() throws JobExecutionException {
//		IndexDocumentTeca admd = null;
		QueryResponse qr = null;
		SolrDocumentList response = null;
		String query = "";
		SolrDocument doc = null;
		FindDocument find = null;
		Vector<String> cols = null;
		Vector<SortClause> sort = null;
		String pathOld = "";
		String path = null;
		String idOld = "";
		String id = null;
		BufferedWriter bw = null;
		FileWriter fw = null;
		File f = null;

		try {
			f = new File("Result.out");
			
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);
//			admd = new IndexDocumentTeca();
			find = new FindDocument("http://mxmuletto2.bncf.lan:8983/solr/tecaAttilioHortisOcr",
					Boolean.parseBoolean("false"),
					"tecaAttilioHortisOcr",
					Integer.parseInt("60000"),
					Integer.parseInt("100000"));
			query = "*:*";
			
			cols = new Vector<String>();
			cols.add(ItemTeca.ID);
			cols.add(ItemTeca.ORIGINALFILENAME+"_show");
			sort = new Vector<SortClause>();
			sort.add(SortClause.desc(ItemTeca.ORIGINALFILENAME+"_sort"));
			qr = find.find(query,0,1000000,cols, sort);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for (int x=0; x<response.getNumFound(); x++){
						doc = response.get(x);
						id = (String) doc.getFieldValue((ItemTeca.ID));
						path = ((String)((ArrayList<Object>)doc.getFieldValues(ItemTeca.ORIGINALFILENAME+"_show")).get(0));
						if (path.trim().equals(pathOld.trim())) {
							bw.write(id+"\t"+path+"\t"+idOld+"\t"+pathOld+"\n");
							System.out.println(id+"\t"+path+"\t"+idOld+"\t"+pathOld);
						}
						pathOld = path;
						idOld = id;
					}
				}
			}
		} catch (NumberFormatException e) {
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (IOException e) {
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrException e) {
			throw new JobExecutionException(e.getMessage(), e, false);
		} catch (SolrServerException e) {
			throw new JobExecutionException(e.getMessage(), e, false);
		} finally {
			try {
				if (find != null){
					find.close();
				}
				if (bw != null) {
					bw.flush();
					bw.close();
				}
				if(fw != null) {
					fw.close();
				}
			} catch (IOException e) {
				throw new JobExecutionException(e.getMessage(), e, false);
			}
		}
		
	}
}
