package mx.teca2015.tecaPublished.standAlone.quartz.file.schede;

import it.sbn.iccu.metaag1.Bib;
import it.sbn.iccu.metaag1.Bib.Holdings;
import it.sbn.iccu.metaag1.Bib.Holdings.Shelfmark;
import it.sbn.iccu.metaag1.BibliographicLevel;
import it.sbn.iccu.metaag1.Gen;
import it.sbn.iccu.metaag1.Img;
import it.sbn.iccu.metaag1.Link;
import it.sbn.iccu.metaag1.Metadigit;
import it.sbn.iccu.metaag1.Img.Altimg;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.GregorianCalendar;
import java.util.Vector;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.niso.pdfs.datadict.ImageCreation;
import org.niso.pdfs.datadict.ImageCreation.Scanningsystem;
import org.purl.dc.elements._1.SimpleLiteral;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import mx.randalf.configuration.Configuration;
import mx.randalf.configuration.exception.ConfigurationException;
import mx.randalf.interfacException.exception.PubblicaException;
import mx.randalf.mag.MagXsd;
import mx.randalf.tools.Utils;
import mx.randalf.tools.exception.UtilException;
import mx.randalf.xsd.exception.XsdException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JFile;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JUC;
import mx.teca2015.tecaPublished.standAlone.quartz.file.exception.JFileException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.xlsx.ToolsXsl;

public class U {
	protected String bid = null;
	protected String soggettoConservatore = null;
	private String soggettoConservatoreKey = null;
	private String pathMag = null;
	private String fondo = null;
	private String fondoKey = null;
	private String subFondo = null;
	private String subFondoKey = null;
	protected String collocazione = null;
	protected String titolo = null;
	protected String lingua = null;
	protected Integer pagine = null;
	protected String supporto = null;
	protected String statoConservazione = null;
	protected String compilatore = null;
	protected String dataCompilazione = null;
	protected String note = null;

	protected Metadigit magIndex = null;

	public U(Cell[] riga, File fOri, File folderIndex) throws JFileException {
		String tmp = null;

		bid =ToolsXsl.analizza(riga[0]);
		if (bid ==null){
			throw new JFileException("Asssente il Codice di identificazione");
		}
		
		soggettoConservatoreKey =  ToolsXsl.analizza(riga[3]);
		pathMag = "."+File.separator+soggettoConservatoreKey;
		
		soggettoConservatore =  ToolsXsl.analizza(riga[4]);
		if (soggettoConservatore.equalsIgnoreCase("Biblioteca Pubblica Arcivescovile \"Annibale De Leo\"") ||
				soggettoConservatore.equalsIgnoreCase("Biblioteca Arcivescovile A. De Leo Brindisi")){
			soggettoConservatore = "Biblioteca pubblica arcivescovile Annibale De Leo";
		}
		if (soggettoConservatoreKey ==null ||
				soggettoConservatore == null){
			throw new JFileException("Asssente il Sogetto conservatore");
		}
		
		fondoKey =  ToolsXsl.analizza(riga[5]);
		pathMag += File.separator+fondoKey;
		
		fondo =  ToolsXsl.analizza(riga[6]);
		if (fondoKey ==null ||
				fondo == null){
			throw new JFileException("Asssente il Fondo");
		}
		
		fondo = fondo.replace("Consiglio d'Intendenza di Capitanata", "Consiglio di Intendenza di Capitanata");
		fondo = fondo.replace("Gran corte criminale di Capitanata", "Gran Corte criminale di Capitanata");
		fondo = fondo.replace("Tribunale civile di Terra d'Otranto", "Tribunale Civile di Terra d'Otranto");
		fondo = fondo.replace("Tribunale civile e penale di Lucera", "Tribunale Civile e Penale di Lucera");
		
		subFondoKey =  ToolsXsl.analizza(riga[7]);
		
		subFondo =  ToolsXsl.analizza(riga[8]);

		if (subFondoKey != null && !subFondoKey.trim().equals("")){
			pathMag += File.separator+subFondoKey;
		}

		if (subFondo != null){
			subFondo = subFondo.replace("Serie II", "II Serie");
			subFondo = subFondo.replace("Archivio comunale di Brindisi-Ctg X-Cl 9", "Archivio comunale di Brindisi-Ctg X-Classe 9");
			subFondo = subFondo.replace("Affari Comunali", "Affari comunali");
			subFondo = subFondo.replace("Archivio comunale di Brindisi-Ctg X-Cl 23", "Archivio comunale di Brindisi-Ctg X-Classe 23");
			subFondo = subFondo.replace("Archivio comunale di Brindisi-Ctg I-Cl 12", "Archivio comunale di Brindisi-Ctg I-Classe 12");
			subFondo = subFondo.replace("Archivio comunale di Brindisi-Ctg. X-Cl25", "Archivio comunale di Brindisi-Ctg X-Classe 25");
			subFondo = subFondo.replace("Archivio comunele di Brindisi-Ctg III-Classe 3", "Archivio comunale di Brindisi-Ctg III-Classe 3");
			subFondo = subFondo.replace("Archivio comunele di Brindisi-Ctg IV-Cl 7", "Archivio comunale di Brindisi-Ctg IV-Cl 7");
			subFondo = subFondo.replace("Ammnistrazione del Beneficio di San Pietro in Cuppis", "Amministrazione del Beneficio di San Pietro in Cuppis");
		}
		pathMag += File.separator+ToolsXsl.analizza(riga[10]);
		
		collocazione =  ToolsXsl.analizza(riga[10]);
		tmp = ToolsXsl.analizza(riga[11]);
		if (collocazione ==null ||
				tmp == null){
			throw new JFileException("Asssente la Collocazione");
		} else {
			pathMag += tmp.replace(".0", "");
			collocazione += " "+tmp.replace(".0", "");
		}

		tmp = ToolsXsl.analizza(riga[13]);
		if (tmp != null){
			pathMag += File.separator+tmp;
			collocazione += " "+tmp;

			tmp = ToolsXsl.analizza(riga[14]);
			if (collocazione ==null ||
					tmp == null){
				throw new JFileException("Asssente la Collocazione");
			} else {
				pathMag += tmp.replace(".0", "");
				collocazione += " "+tmp.replace(".0", "");
			}

			tmp = ToolsXsl.analizza(riga[15]);
			if (tmp != null){
				pathMag += "."+tmp;
				collocazione += "."+tmp;
			}
		}

		titolo =  ToolsXsl.analizza(riga[17]);
		if (titolo.equalsIgnoreCase("\"Quarto quadro delle trapizio di Gurone\"")){
			titolo = "\"Quarto quadro del trapizzo di Gurone\"";
		}
		if (titolo.equalsIgnoreCase("\"Pianta di tutto il il qui sottoscritto comprensorio chiamato la Difesa di Femmina Morta\"")){
			titolo = "\"Pianta di tutto il qui sottoscritto comprensorio chiamato la Difesa di Femmina Morta\"";
		}
		if (titolo.equalsIgnoreCase("\"Pianta topografica della tenuta boscosa denominata Niuzi in tenimento di Ischitella colla indicazione delle contrade, delle classi del terreno, e del partaggio fattone\"")){
			titolo = "\"Pianta corografica della tenuta boscosa denominata Niuzi in tenimento di Ischitella colla indicazione delle Contrade, delle classi del terreno, e del partaggio fattone\"";
		}
		if (titolo.equalsIgnoreCase("\"Locatione di canosa\"")){
			titolo = "\"Locatione di Canosa\"";
		}
		if (titolo ==null){
			throw new JFileException("Asssente il Titolo");
		}

		lingua =  ToolsXsl.analizza(riga[18]);
		if (lingua ==null){
			throw new JFileException("Asssente la Lingua");
		}
	}

	public static Metadigit genMag(File fOri, File folderIndex, boolean isIIPImage, boolean genMagComp, 
			boolean genMagPub, Integer pagine, Metadigit magIndex, String bid, 
			String titolo, String lingua, String collocazione, String soggettoConservatore,  
			Vector<String> autori) throws JFileException{
		Workbook wb = null;
		Sheet finestra = null;
		Cell[] riga = null;
		File f = null;
		File fMag = null;
		File fMagIndex = null;
		Gen gen = null;
		Bib bib = null;
		SimpleLiteral identifier = null;
		SimpleLiteral title = null;
		SimpleLiteral language = null;
		SimpleLiteral autore = null;
		SimpleLiteral subject = null;
		Holdings holdings = null;
		Shelfmark shelfmark = null;
		MagXsd magXsd = null;
		Img img = null;
		String[] st = null;
		String folderImg = null;
		File fImg = null;
		Link file = null;
		Altimg altImg = null;
		String fileOri = null;
		String fileDes = null;
		String fileTif = null;
		String fileIIPImage = null;
		int sequenceNumber = 0;
		Metadigit mag = null;
		int numRighe = 0;

		try {
			st = fOri.getName().replace(".xls", "").split("_");

			if (st[st.length-2].startsWith("UD")){
				fImg = new File(fOri.getParentFile().getAbsolutePath()+
						File.separator+
						st[st.length-2]);
				if (fImg.exists()){
					folderImg = "./"+st[st.length-2]+"/";
				} else {
					folderImg = "./";
				}
			} else {
				fImg = new File(fOri.getParentFile().getAbsolutePath()+
						File.separator+
						st[st.length-1]);
				if (fImg.exists()){
					folderImg = "./"+st[st.length-1]+"/";
				} else {
					fImg = new File(fOri.getParentFile().getAbsolutePath()+
							File.separator+
							st[st.length-2]);
					if (fImg.exists()){
						folderImg = "./"+st[st.length-2]+"/";
					} else {
						folderImg = "./";
					}
				}
			}
			f = new File(fOri.getParentFile().getAbsolutePath()+
					File.separator+
					fOri.getName().replace(".xls", "_Nomenclatura.xls"));
			fMag = new File(fOri.getParentFile().getAbsolutePath()+
					File.separator+
					fOri.getName().replace(" ", "_").replace(".xls", "_mag.xml"));
			fMagIndex = new File(folderIndex.getAbsolutePath()+
					File.separator+
					fOri.getName().replace(" ", "_").replace(".xls", "_mag.xml"));
			if (f.exists()){
				// Apro il file xlsx
				wb = JUC.openFileXls(f.getAbsolutePath());

				// Leggo le infomrazioni della 1 finestra
				finestra = wb.getSheet(0);
				numRighe = numRig(finestra);
				if (pagine == null || numRighe==pagine){
					mag = new Metadigit();
					magIndex = new Metadigit();

					gen = new Gen();
					gen.setAccessRights(new BigInteger("1"));
					gen.setAgency("SEGRETARIATO REGIONALE DEL MINISTERO DEI BENI E DELLE ATTIVITA' CULTURALI E DEL TURISMO PER LA PUGLIA");
					gen.setCollection("http://www.beniculturali.it/mibac/export/MiBAC/sito-MiBAC/Luogo/MibacUnif/Enti/visualizza_asset.html_2000168143.html");
					gen.setCompleteness(new BigInteger("0"));
					gen.setCreation(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()));
					gen.setLastUpdate(DatatypeFactory.newInstance().newXMLGregorianCalendar(new GregorianCalendar()));
					gen.setStprog("http://www.beniculturali.it/mibac/export/MiBAC/sito-MiBAC/Luogo/MibacUnif/Enti/visualizza_asset.html_2000168143.html");
					mag.setGen(gen);
					magIndex.setGen(gen);
					
					bib = new Bib();
					bib.setLevel(BibliographicLevel.M);
					identifier  = new SimpleLiteral();
					identifier.getContent().add(bid);
					bib.getIdentifier().add(identifier);

					title = new SimpleLiteral();
					title.getContent().add(titolo);
					bib.getTitle().add(title);

					if (autori != null && autori.size()>0){
						for (int x=0;x <autori.size(); x++){
							
							autore = new SimpleLiteral();
							autore.getContent().add(autori.get(x));
							bib.getCreator().add(autore);
						}
					}
					if (lingua != null && !lingua.trim().equals("")){
						language = new SimpleLiteral();
						language.getContent().add(lingua);
						bib.getLanguage().add(language);
					}

					if (soggettoConservatore != null && !soggettoConservatore.trim().equals("")){
						subject = new SimpleLiteral();
						subject.getContent().add(soggettoConservatore);
						bib.getSubject().add(subject);
					}


					if (collocazione != null && !collocazione.trim().equals("")){

						holdings = new Holdings();
						shelfmark = new Shelfmark();
						shelfmark.setContent(collocazione);
						holdings.getShelfmark().add(shelfmark);
						bib.getHoldings().add(holdings);
					}
					
					
					mag.setBib(bib);
					magIndex.setBib(bib);
					
					magXsd = new MagXsd();
					for (int x=1; x<finestra.getRows(); x++){
						riga = finestra.getRow(x);
						if (riga.length==2 &&
								ToolsXsl.analizza(riga[0]) != null &&
								ToolsXsl.analizza(riga[1]) != null){
							sequenceNumber++;
							if (genMagPub){
								img = new Img();
								img.setSequenceNumber(new BigInteger(Integer.toString(sequenceNumber)));
								img.setNomenclature(ToolsXsl.analizza(riga[1]));
								img.getUsage().add("3");
								file = new Link();
								file.setHref(folderImg+"150/"+ToolsXsl.analizza(riga[0]).trim().replace(".tif", ".jpg"));
								img.setFile(file);
								fileOri = fOri.getParentFile().getAbsolutePath() +
										File.separator+
										folderImg.replace("./", "")+
										"150/"+ToolsXsl.analizza(riga[0]).trim().replace(".tif", ".jpg");
								fileDes = folderIndex.getAbsolutePath() +
										File.separator+
										folderImg.replace("./", "")+
										"150/"+ToolsXsl.analizza(riga[0]).trim().replace(".tif", ".jpg");
								if (!new File(fileOri).exists()){
									throw new JFileException("Il file ["+
											fileOri+"] non esiste ");
								}
								if (Configuration.getValueDefault("aggImg", "true").equals("true")){
									if (!Utils.copyFileValidate(fileOri, fileDes, false)){
										throw new JFileException("Problemi nella copia del file ["+
												fileOri+"] in ["+fileDes+"]");
									}
								}
								
								if (isIIPImage){
									fileTif = fOri.getParentFile().getAbsolutePath() +
											File.separator+
											folderImg.replace("./", "")+
											"TIF/"+ToolsXsl.analizza(riga[0]).trim();
									fileIIPImage = folderIndex.getAbsolutePath() +
											File.separator+
											folderImg.replace("./", "")+
											"IIPImage/"+ToolsXsl.analizza(riga[0]).trim();
									File fIIPImage = new File(fileIIPImage);
									if (!fIIPImage.exists()){
										if (Configuration.getValueDefault("aggImg", "true").equals("true")){
											if (!JFile.convertImg(new File(fileTif), fIIPImage)){
												throw new JFileException("Problemi nella conversione del file ["+
														fileTif+"] in ["+fileIIPImage+"]");
											}
										}
									}
									
									altImg = new Altimg();
									altImg.getUsage().add("6");
									file = new Link();
									file.setHref(folderImg+"IIPImage/"+ToolsXsl.analizza(riga[0]).trim());
									altImg.setFile(file);
									img.getAltimg().add(altImg);
								}
								
								try {
								magXsd.calcImg(img, folderIndex.getAbsolutePath());
								} catch(Exception e){
									
								}
								magIndex.getImg().add(img);
							}

							if (genMagComp){
								img = new Img();
								img.setSequenceNumber(new BigInteger(Integer.toString(sequenceNumber)));
								img.setNomenclature(ToolsXsl.analizza(riga[1]));
								img.getUsage().add("1");
								file = new Link();
								file.setHref(folderImg+"TIF/"+ToolsXsl.analizza(riga[0]).trim());
								img.setFile(file);
								System.out.println("File: "+file.getHref());
		
								altImg = new Altimg();
								altImg.getUsage().add("2");
								file = new Link();
								file.setHref(folderImg+"300/"+ToolsXsl.analizza(riga[0]).trim().replace(".tif", ".jpg"));
								altImg.setFile(file);
								img.getAltimg().add(altImg);
		
								altImg = new Altimg();
								altImg.getUsage().add("3");
								file = new Link();
								file.setHref(folderImg+"150/"+ToolsXsl.analizza(riga[0]).trim().replace(".tif", ".jpg"));
								altImg.setFile(file);
								img.getAltimg().add(altImg);
								magXsd.calcImg(img, fOri.getParentFile().getAbsolutePath());
								ImageCreation scanning = null;
								scanning = new ImageCreation();
								scanning.setSourcetype("scanner");
								scanning.setScanningagency("Present S.p.A.");
								scanning.setDevicesource("scanner");
								Scanningsystem scanningsystem = null;
								scanningsystem = new Scanningsystem();
								scanningsystem.setScannerManufacturer("Metis");
								scanningsystem.setCaptureSoftware("EDS Software 2.57");
								scanningsystem.setScannerModel("Metis 2.57");
								scanning.setScanningsystem(scanningsystem);
								img.setScanning(scanning);
								mag.getImg().add(img);
							}
						}
					}

					if (genMagComp){
						magXsd.write(mag, fMag);
					}
					if (genMagPub){
						magXsd.write(magIndex, fMagIndex);
					}
				} else {
					throw new JFileException("Il numero di pagine ["+
							pagine+
							"] indicate come digitalizzate non corrispondono a quelle nomenclate ["+
							numRighe+
							"]");
				}
			} else {
				throw new JFileException("Il file ["+f.getAbsolutePath()+"] non esiste");
			}
		} catch (BiffException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (IndexOutOfBoundsException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (DatatypeConfigurationException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (XsdException e) {
			e.printStackTrace();
			throw new JFileException(e.getMessage(), e);
		} catch (JFileException e) {
			throw e;
		} catch (UtilException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (PubblicaException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (NullPointerException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (ConfigurationException e) {
			throw new JFileException(e.getMessage(), e);
		} finally {
			if (wb != null){
				wb.close();
			}
		}
		return magIndex;
	}

	private static int numRig(Sheet finestra){
		Cell[] riga = null;
		int conta = 0;
		for (int x=1; x<finestra.getRows(); x++){
			riga = finestra.getRow(x);
			if (riga.length==2 &&
					ToolsXsl.analizza(riga[0]) != null &&
					ToolsXsl.analizza(riga[1]) != null){
				conta++;
			}
		}
		return conta;
	}
	/**
	 * @return the bid
	 */
	public String getBid() {
		return bid;
	}

	/**
	 * @return the soggettoConservatore
	 */
	public String getSoggettoConservatore() {
		return soggettoConservatore;
	}

	/**
	 * @return the soggettoConservatoreKey
	 */
	public String getSoggettoConservatoreKey() {
		return soggettoConservatoreKey;
	}

	/**
	 * @return the pathMag
	 */
	public String getPathMag() {
		return pathMag;
	}

	/**
	 * @return the fondo
	 */
	public String getFondo() {
		return fondo;
	}

	/**
	 * @return the fondoKey
	 */
	public String getFondoKey() {
		return fondoKey;
	}

	/**
	 * @return the subFondo
	 */
	public String getSubFondo() {
		return subFondo;
	}

	/**
	 * @return the subFondoKey
	 */
	public String getSubFondoKey() {
		return subFondoKey;
	}

	/**
	 * @return the collocazione
	 */
	public String getCollocazione() {
		return collocazione;
	}

	/**
	 * @return the titolo
	 */
	public String getTitolo() {
		return titolo;
	}

	/**
	 * @return the lingua
	 */
	public String getLingua() {
		return lingua;
	}

	/**
	 * @return the pagine
	 */
	public Integer getPagine() {
		return pagine;
	}

	/**
	 * @return the supporto
	 */
	public String getSupporto() {
		return supporto;
	}

	/**
	 * @return the statoConservazione
	 */
	public String getStatoConservazione() {
		return statoConservazione;
	}

	/**
	 * @return the compilatore
	 */
	public String getCompilatore() {
		return compilatore;
	}

	/**
	 * @return the dataCompilazione
	 */
	public String getDataCompilazione() {
		return dataCompilazione;
	}

	/**
	 * @return the note
	 */
	public String getNote() {
		return note;
	}

	/**
	 * @return the magIndex
	 */
	public Metadigit getMagIndex() {
		return magIndex;
	}

}
