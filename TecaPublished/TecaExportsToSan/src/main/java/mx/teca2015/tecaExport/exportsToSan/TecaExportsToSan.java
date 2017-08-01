/**
 * 
 */
package mx.teca2015.tecaExport.exportsToSan;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Vector;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.xml.sax.SAXParseException;

import it.mibac.san.ComplessiArchivistici;
import it.mibac.san.ComplessiArchivisticiDati;
import it.mibac.san.SoggettoConservatore;
import it.mibac.san.SoggettoConservatoreDati;
import it.mibac.san.SoggettoProduttore;
import it.mibac.san.SoggettoProduttoreDati;
import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.solr.FindDocument;
import mx.randalf.solr.exception.SolrException;
import mx.randalf.xsd.exception.XsdException;
import mx.teca2015.tecaUtility.solr.item.ItemTeca;

/**
 * @author massi
 *
 */
public class TecaExportsToSan {

	private static Logger log = Logger.getLogger(TecaExportsToSan.class);

	private Vector<String> idSoggettiConservatore = new Vector<String>();

	private SoggettoConservatore soggettoConservatore = null;

	private Vector<String> idComplessiArchivistici = new Vector<String>();

	private ComplessiArchivistici complessiArchivistici = null;

	private Vector<String> idSoggettiProduttori = new Vector<String>();

	private SoggettoProduttore soggettiProduttori = null;
	
	/**
	 * @throws ConfigurationException 
	 * 
	 */
	public TecaExportsToSan(String pathConfigurazione) throws ConfigurationException {
		Configuration.init(pathConfigurazione);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TecaExportsToSan tecaExportsToSan = null;

		try {
			if (args.length>=2){
				tecaExportsToSan = new TecaExportsToSan(args[0]);
				for (int x=1; x<args.length; x++){
					tecaExportsToSan.addIdSoggettoConservatore(args[x]);
				}
				tecaExportsToSan.esegui();
			} else {
				System.out.println("E' necessario indicare il seguenti parametri");
				System.out.println("1) Path files configurazione");
				System.out.println("2) Id Soggetto Conservatore (Campo riperibile)");
			}
		} catch (ConfigurationException e) {
			log.error(e.getMessage(), e);
		} catch (DatatypeConfigurationException e) {
			log.error(e.getMessage(), e);
		} catch (SAXParseException e) {
			log.error(e.getMessage(), e);
		} catch (NoSuchAlgorithmException e) {
			log.error(e.getMessage(), e);
		} catch (XsdException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} catch (SolrException e) {
			log.error(e.getMessage(), e);
		} catch (SolrServerException e) {
			log.error(e.getMessage(), e);
		}
	}

	public void addIdSoggettoConservatore(String idSoggettoConservatore){
		idSoggettiConservatore.add(idSoggettoConservatore);
	}

	public void esegui() throws DatatypeConfigurationException, ConfigurationException, 
			SAXParseException, NoSuchAlgorithmException, XsdException, IOException, SolrException, SolrServerException{
		try {

			soggettoConservatore = new SoggettoConservatore(
					Configuration.getValue("systemId"),
					Configuration.getValue("contact.name"),
					Configuration.getValue("contact.email"),
					Configuration.getValue("contact.phone"),
					Configuration.getValue("filedesc.title"),
					Configuration.getValue("filedesc.abstract"));

			complessiArchivistici = new ComplessiArchivistici(
					Configuration.getValue("systemId"),
					Configuration.getValue("contact.name"),
					Configuration.getValue("contact.email"),
					Configuration.getValue("contact.phone"),
					Configuration.getValue("filedesc.title"),
					Configuration.getValue("filedesc.abstract"));

			soggettiProduttori = new SoggettoProduttore(
					Configuration.getValue("systemId"),
					Configuration.getValue("contact.name"),
					Configuration.getValue("contact.email"),
					Configuration.getValue("contact.phone"),
					Configuration.getValue("filedesc.title"),
					Configuration.getValue("filedesc.abstract"));

			for (int x=0; x<idSoggettiConservatore.size(); x++){
				System.out.println("Inizio elaborazione: "+idSoggettiConservatore.get(x));
				findSoggettoConsergatore(idSoggettiConservatore.get(x));
				System.out.println("Fine elaborazione: "+idSoggettiConservatore.get(x));
			}
			
			soggettoConservatore.write(new File(Configuration.getValue("pathExport")+
					File.separator+
					Configuration.getValue("fileSoggettoConservatoreOutput")
					));
			
			complessiArchivistici.write(new File(Configuration.getValue("pathExport")+
					File.separator+
					Configuration.getValue("fileComplessoArchivisticoOutput")
					));
			
			soggettiProduttori.write(new File(Configuration.getValue("pathExport")+
					File.separator+
					Configuration.getValue("fileSoggettoProduttoreOutput")
					));
		} catch (DatatypeConfigurationException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (SAXParseException e) {
			throw e;
		} catch (NoSuchAlgorithmException e) {
			throw e;
		} catch (XsdException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (SolrException e) {
			throw e;
		} catch (SolrServerException e) {
			throw e;
		}
	}

	private void findSoggettoConsergatore(String idSoggettoConsergatore) throws SolrException, ConfigurationException, SolrServerException, IOException, DatatypeConfigurationException{
		FindDocument find = null;
		String query = null;
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SoggettoConservatoreDati soggettoConservatoreDati = null;
		SolrDocument record = null;
		String indirizzo = null;
		String[] st = null;
		String telefonoFax = null;
		GregorianCalendar gc = null;
		ArrayList<Object> complessiArchivisticiID  = null;

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
			query = "+"+ItemTeca.BID+":\""+idSoggettoConsergatore+"\" "+
							"+"+ItemTeca.TIPOLOGIAFILE+":\""+ItemTeca.TIPOLOGIAFILE_SOGGETTOCONSERVATORE+"\"";
			
			qr = find.find(query,0,1);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for ( int x=0; x<response.getNumFound(); x++){
						record = response.get(x);
						if (record.getFirstValue(ItemTeca.BID+"_show").
								equals(idSoggettoConsergatore)){
							soggettoConservatoreDati = new SoggettoConservatoreDati();

							soggettoConservatoreDati.setIstituto(
									(String)record.getFirstValue(ItemTeca.TITOLO+"_show"));
							
							soggettoConservatoreDati.setUrlId(
									Configuration.getValue("urlSoggettoConservatore")+
									(String)record.getFirstValue(ItemTeca.BID+"_show"));
							
							soggettoConservatoreDati.setId(
									(String)record.getFirstValue(ItemTeca.BID+"_show"));

							soggettoConservatoreDati.setTipoSoggettoConservatore(
									(String)record.getFirstValue(ItemTeca.TIPOSOGGETTOCONSERVATORE+"_show"));

							indirizzo = (String) record.getFirstValue(ItemTeca.INDIRIZZO+"_show");
							System.out.println("Indirizzo: "+indirizzo);
							indirizzo = indirizzo.replace(" -76125", " - 76125");
							st = indirizzo.split(" - ");
							
							soggettoConservatoreDati.setIndirizzo(st[0]);
							
							st = st[1].split(" ");
							soggettoConservatoreDati.setCap(new BigInteger(st[0].replace(",", "")));
							soggettoConservatoreDati.setProvincia(st[1]);
							soggettoConservatoreDati.setComune(st[2].
									replace("(", "").
									replace(")", ""));
							soggettoConservatoreDati.setPaese("Italia");

							telefonoFax = "";
							if (record.getFirstValue(ItemTeca.TELEFONO+"_show")!= null){
								telefonoFax = "Telefono: "+record.getFirstValue(ItemTeca.TELEFONO+"_show");
							}
							if (record.getFirstValue(ItemTeca.FAX+"_show")!= null){
								telefonoFax += (telefonoFax.equals("")?"":" ");
								telefonoFax += "Fax: "+record.getFirstValue(ItemTeca.FAX+"_show");
							}
							soggettoConservatoreDati.setTelefonoFax(telefonoFax);

							soggettoConservatoreDati.setDescrizione(
										(String) record.getFirstValue(ItemTeca.DESCRIZIONE+"_show"));

							soggettoConservatoreDati.setOrarioApertura(
									(String) record.getFirstValue(ItemTeca.ORARIOAPERTURA+"_show"));
						
							soggettoConservatoreDati.setServizioConsultazione(
									((String) record.getFirstValue(ItemTeca.SERVIZIOPUB+"_show")).equalsIgnoreCase("Si"));
							
							gc = new GregorianCalendar();
							gc.setTime((Date) record.get("indexed"));
							soggettoConservatore.add(idSoggettoConsergatore, 
									DatatypeFactory.
										newInstance().
											newXMLGregorianCalendar(gc), 
									soggettoConservatoreDati);

							complessiArchivisticiID =(ArrayList<Object>) record.getFieldValues(ItemTeca.CHILDREN+"_show");
							for (int y=0; y<complessiArchivisticiID.size(); y++){
								if (!complessiArchivisticiID.get(y).equals("none")){
									if (!idComplessiArchivistici.contains(idSoggettoConsergatore+"."+complessiArchivisticiID.get(y))){
										System.out.println("Inizio elaborazione "+idSoggettoConsergatore+"."+complessiArchivisticiID.get(y));
										findComplessoArchivistico(idSoggettoConsergatore, (String) complessiArchivisticiID.get(y));
										System.out.println("Fine elaborazione "+idSoggettoConsergatore+"."+complessiArchivisticiID.get(y));
										idComplessiArchivistici.add(idSoggettoConsergatore+"."+complessiArchivisticiID.get(y));
									}
								}
							}
						}
					}
				}
			}
		} catch (NumberFormatException e) {
			throw e;
		} catch (SolrException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (SolrServerException e) {
			throw e;
		} catch (DatatypeConfigurationException e) {
			throw e;
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}

	private void findComplessoArchivistico(String idSoggettoConsergatore, String idComplessoArchivistico) throws SolrException, ConfigurationException, SolrServerException, IOException, DatatypeConfigurationException{
		FindDocument find = null;
		String query = null;
		QueryResponse qr = null;
		SolrDocumentList response = null;
		ComplessiArchivisticiDati complessiArchivisticiDati = null;
		SolrDocument record = null;
		GregorianCalendar gc = null;
		ArrayList<Object> soggettiProduttoriId  = null;

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
			query = "+"+ItemTeca.BID+":\""+idSoggettoConsergatore+"."+idComplessoArchivistico+"\" "+
							"+"+ItemTeca.TIPOLOGIAFILE+":\""+ItemTeca.TIPOLOGIAFILE_COMPLESSOARCHIVISTICO+"\"";
			
			qr = find.find(query,0,1);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for ( int x=0; x<response.getNumFound(); x++){
						record = response.get(x);
						if (record.getFirstValue(ItemTeca.BID+"_show").
								equals(idSoggettoConsergatore+"."+idComplessoArchivistico)){

							complessiArchivisticiDati = new ComplessiArchivisticiDati();

							
							complessiArchivisticiDati.setUrlId(
									Configuration.getValue("urlSoggettoConservatore")+
									(String)record.getFirstValue(ItemTeca.BID+"_show"));
							
							complessiArchivisticiDati.setId(
									(String)record.getFirstValue(ItemTeca.BID+"_show"));
							
							complessiArchivisticiDati.setTitolo(
									(String)record.getFirstValue(ItemTeca.TITOLO+"_show"));

							complessiArchivisticiDati.setDescrizione(
									(String)record.getFirstValue(ItemTeca.DESCRIZIONE+"_show"));

							complessiArchivisticiDati.setEstremi(
									(String)record.getFirstValue(ItemTeca.ESTREMI+"_show"));

							complessiArchivisticiDati.setConsistenza(
									(String)record.getFirstValue(ItemTeca.CONSISTENZACARTE+"_show"));

							soggettiProduttoriId = (ArrayList<Object>) record.getFieldValues(ItemTeca.SOGGETTOPRODUTTOREKEY+"_show");

							if (soggettiProduttoriId != null){
								for (int y=0; y<soggettiProduttoriId.size(); y++){
									complessiArchivisticiDati.addSoggettoProduttore((String) soggettiProduttoriId.get(y));
									if (!idSoggettiProduttori.contains(soggettiProduttoriId.get(y))){
										System.out.println("Inizio Compilazione: "+soggettiProduttoriId.get(y));
										findSoggettoProduttore((String) soggettiProduttoriId.get(y));
										System.out.println("Fine Compilazione: "+soggettiProduttoriId.get(y));
										idSoggettiProduttori.add((String) soggettiProduttoriId.get(y));
									}
								}
							}

							complessiArchivisticiDati.setSoggettoConservatoreId(
									(String)record.getFirstValue(ItemTeca.SOGGETTOCONSERVATOREKEY+"_show"));
							complessiArchivisticiDati.setSoggettoConservatore(
									(String)record.getFirstValue(ItemTeca.SOGGETTOCONSERVATORE+"_show"));
							
							complessiArchivisticiDati.setProcessinfo("scheda pubblicata");

							complessiArchivisticiDati.setTipologia(
									(String)record.getFirstValue(ItemTeca.TIPOLOGIA+"_show"));

							gc = new GregorianCalendar();
							gc.setTime((Date) record.get("indexed"));
//							System.out.println("complessiArchivistici: "+complessiArchivistici+
//									"\tidSoggettoConsergatore: "+idSoggettoConsergatore+
//									"\tidComplessoArchivistico: "+idComplessoArchivistico+
//									"\tgc: "+gc+
//									"\tcomplessiArchivisticiDati: "+complessiArchivisticiDati);
							complessiArchivistici.add(idSoggettoConsergatore+"."+idComplessoArchivistico, 
									DatatypeFactory.
										newInstance().
											newXMLGregorianCalendar(gc), 
											complessiArchivisticiDati);
						}
					}
				}
			}
		} catch (NumberFormatException e) {
			throw e;
		} catch (SolrException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (SolrServerException e) {
			throw e;
		} catch (DatatypeConfigurationException e) {
			throw e;
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}

	private void findSoggettoProduttore(String idSoggettoProduttore) throws SolrException, ConfigurationException, SolrServerException, IOException, DatatypeConfigurationException{
		FindDocument find = null;
		String query = null;
		QueryResponse qr = null;
		SolrDocumentList response = null;
		SoggettoProduttoreDati soggettoProduttoreDati = null;
		SolrDocument record = null;
		GregorianCalendar gc = null;

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
			query = "+"+ItemTeca.BID+":\""+idSoggettoProduttore+"\" "+
							"+"+ItemTeca.TIPOLOGIAFILE+":\""+ItemTeca.TIPOLOGIAFILE_SOGGETTOPRODUTTORE+"\"";
			
			qr = find.find(query,0,1);
			if (qr.getResponse() != null &&
					qr.getResponse().get("response")!=null){
				response = (SolrDocumentList) qr.getResponse().get("response");
				if (response.getNumFound()>0){
					for ( int x=0; x<response.getNumFound(); x++){
						record = response.get(x);
						if (record.getFirstValue(ItemTeca.BID+"_show").
								equals(idSoggettoProduttore)){

							soggettoProduttoreDati = new SoggettoProduttoreDati();
							
							soggettoProduttoreDati.setUrlId(
									Configuration.getValue("urlSoggettoConservatore")+
									(String)record.getFirstValue(ItemTeca.BID+"_show"));
							
							soggettoProduttoreDati.setId(
									(String)record.getFirstValue(ItemTeca.BID+"_show"));

							soggettoProduttoreDati.setTitolo(
									(String)record.getFirstValue(ItemTeca.TITOLO+"_show"));

							soggettoProduttoreDati.setDataIstituzione(
									(String)record.getFirstValue(ItemTeca.DATAESISTENZA+"_show"));
							
							soggettoProduttoreDati.setDataSospensione(
									(String)record.getFirstValue(ItemTeca.DATAMORTE+"_show"));

							soggettoProduttoreDati.setSede(
									(String)record.getFirstValue(ItemTeca.SEDE+"_show"));

							soggettoProduttoreDati.setTipoEnte(
									(String)record.getFirstValue(ItemTeca.TIPOENTE+"_show"));

							soggettoProduttoreDati.setDescrizione(
									(String)record.getFirstValue(ItemTeca.DESCRIZIONE+"_show"));

							soggettoProduttoreDati.setComplessoArchivisticoId(
									(String)record.getFirstValue(ItemTeca.FONDOKEY+"_show"));
							
							gc = new GregorianCalendar();
							gc.setTime((Date) record.get("indexed"));
							soggettiProduttori.add(idSoggettoProduttore, 
									DatatypeFactory.
										newInstance().
											newXMLGregorianCalendar(gc), 
											soggettoProduttoreDati);
						}
					}
				}
			}
		} catch (NumberFormatException e) {
			throw e;
		} catch (SolrException e) {
			throw e;
		} catch (ConfigurationException e) {
			throw e;
		} catch (SolrServerException e) {
			throw e;
		} catch (DatatypeConfigurationException e) {
			throw e;
		} finally {
			try {
				if (find != null){
					find.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}
	
}