/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file.schede;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.JUC;
import mx.teca2015.tecaPublished.standAlone.quartz.file.exception.JFileException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.xlsx.ToolsXsl;

/**
 * @author massi
 *
 */
public class UC extends U {

	private String root = null;
	private String rootDesc = null;
	private String tipologiaMateriale = null;
	private String dataCronica = null;
	private String dataTopica = null;
	private String[] tecniche = null;
	private String dimensione = null;
	private String scala = null;
	private Vector<String> autori = null;
	private String[] datiFruizione = null;

	/**
	 * 
	 */
	public UC(Cell[] riga, File fOri, File folderIndex, boolean genMagComp, boolean genMagPub) throws JFileException{
		super(riga, fOri, folderIndex);
		String tmp = null;
		String[] values = null;
		String[] qualifiche = null;
		
		String autore = null;

		try {

			root =  ToolsXsl.analizza(riga[2]);
			if (root != null){
				readRoot(fOri);
			}

			tipologiaMateriale =  ToolsXsl.analizza(riga[16]);
			if (tipologiaMateriale ==null){
				throw new JFileException("Asssente la Tipologia di materiale");
			}

			dataCronica = ToolsXsl.analizza(riga[19]);
			if (dataCronica == null || dataCronica.trim().equals("")){
				dataCronica = ToolsXsl.analizza(riga[20]);
				if (dataCronica == null || dataCronica.trim().equals("")){
					dataCronica = ToolsXsl.analizza(riga[21]);
					if (dataCronica == null || dataCronica.trim().equals("")){
						dataCronica = ToolsXsl.analizza(riga[22]);
						if (dataCronica == null || dataCronica.trim().equals("")){
							throw new JFileException("Asssente la Data cronica");
						}
					}
				} else {
					dataCronica = dataCronica.replace(".0", "");
				}
			}

			dataTopica = ToolsXsl.analizza(riga[23]);

			supporto =  ToolsXsl.analizza(riga[26]);
			if (supporto ==null){
				System.out.println(bid+" Asssente il Supporto");
			}

			if (ToolsXsl.analizza(riga[27]) == null){
				throw new JFileException("Asssente la Tecnica");
			} else {
				tecniche = ToolsXsl.analizza(riga[27]).split(" ; ");
			}

			if (ToolsXsl.analizza(riga[28]) == null ||
					ToolsXsl.analizza(riga[29]) == null){
				throw new JFileException("Asssente la Dimensione");
			} else {
				dimensione = ToolsXsl.analizza(riga[28]).replace(".0", "")+" x "+ToolsXsl.analizza(riga[29]).replace(".0", "");
			}

			scala =  ToolsXsl.analizza(riga[30]);
			if (scala ==null){
				scala = "cm";
//				throw new JFileException("Asssente la Scala");
			}

			statoConservazione =  ToolsXsl.analizza(riga[31]);
			if (statoConservazione ==null){
				throw new JFileException("Asssente lo Stato di conservazione");
			}

			if (ToolsXsl.analizza(riga[24]) != null ||
					ToolsXsl.analizza(riga[25]) != null){
				if (!ToolsXsl.analizza(riga[24]).trim().equals("")){
					values = ToolsXsl.analizza(riga[24]).split(" ; ");
					if (!ToolsXsl.analizza(riga[25]).trim().equals("")){
						qualifiche = ToolsXsl.analizza(riga[25]).split(" ; ");
					} else {
						qualifiche = null;
					}
					if (qualifiche == null || values.length==qualifiche.length){
						autori = new Vector<String>();
						for (int x=0; x<values.length;x++){
							autore = values[x].trim();
							if (qualifiche != null){
								autore += (qualifiche[x].trim().equals("")?"":" ("+qualifiche[x].trim()+")");
							}
							if (!autore.trim().equals("")){
								autori.add(autore);
							}
						}
					} else {
						throw new JFileException("Il numero di Autori non corrisponde alle qualifiche indicate");
					}
				}
			}

			tmp =  ToolsXsl.analizza(riga[33]);
			if (tmp ==null){
				throw new JFileException("Asssente i dati di fruizione");
			} else {
				datiFruizione =tmp.split(" ; ");
			}

			compilatore =  ToolsXsl.analizza(riga[35]);
			if (compilatore ==null){
				throw new JFileException("Asssente il nome del compilatore");
			}

			dataCompilazione =  ToolsXsl.analizza(riga[36]);
			if (dataCompilazione ==null){
				throw new JFileException("Asssente la data compilazione");
			}

			note =  ToolsXsl.analizza(riga[32]);
			
			if (ToolsXsl.analizza(riga[34]) !=null){
//				throw new JFileException("Asssente il numero di pagine associate");
//			} else {
				if (!ToolsXsl.analizza(riga[34]).equals("")){
					pagine = new Integer(ToolsXsl.analizza(riga[34]).replace(".0", ""));
				}
			}
			magIndex = genMag(fOri, folderIndex, true, genMagComp, genMagPub, pagine, magIndex, bid, titolo, 
					lingua,collocazione, soggettoConservatore, autori);
		} catch (NumberFormatException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (JFileException e) {
			throw e;
		}
	}

	private void readRoot(File fOri) throws JFileException{
		Workbook wb = null;
		Sheet finestra = null;
		Cell[] riga = null;
		File f = null;
		
		try {
			if (!root.trim().equals("")){
				f = new File(fOri.getParentFile().getAbsolutePath()+
						File.separator+
						root.trim()+".xls");
				if (f.exists()){
					// Apro il file xlsx
					wb = JUC.openFileXls(f.getAbsolutePath());
	
					// Leggo le infomrazioni della 1 finestra
					finestra = wb.getSheet(0);
					riga = finestra.getRow(1);
					rootDesc = ToolsXsl.analizza(riga[17]);
				} else {
					throw new JFileException("Il file ["+f.getAbsolutePath()+"] non esiste");
				}
			}
		} catch (BiffException e) {
			throw new JFileException(e.getMessage(),e);
		} catch (IndexOutOfBoundsException e) {
			throw new JFileException(e.getMessage(),e);
		} catch (IOException e) {
			throw new JFileException(e.getMessage(),e);
		} catch (JFileException e) {
			throw e;
		}  finally {
			if (wb != null){
				wb.close();
			}
		}
	}

	/**
	 * @return the root
	 */
	public String getRoot() {
		return root;
	}

	/**
	 * @return the rootDesc
	 */
	public String getRootDesc() {
		return rootDesc;
	}

	/**
	 * @return the tipologiaMateriale
	 */
	public String getTipologiaMateriale() {
		return tipologiaMateriale;
	}

	/**
	 * @return the dataCronica
	 */
	public String getDataCronica() {
		String[] st = null;
		System.out.println("dataCronica: "+dataCronica);
		if (dataCronica.length()==10){
			st = dataCronica.split("/");
			if (st.length==3){
				if (st[0].length()==4){
					dataCronica = st[2]+"/"+st[1]+"/"+st[0];
				}
			}
		}
		System.out.println("dataCronica: "+dataCronica);
		return dataCronica;
	}

	/**
	 * @return the dataTopica
	 */
	public String getDataTopica() {
		return dataTopica;
	}

	/**
	 * @return the tecniche
	 */
	public String[] getTecniche() {
		return tecniche;
	}

	/**
	 * @return the dimensione
	 */
	public String getDimensione() {
		return dimensione;
	}

	/**
	 * @return the scala
	 */
	public String getScala() {
		return scala;
	}

	/**
	 * @return the autori
	 */
	public Vector<String> getAutori() {
		return autori;
	}

	/**
	 * @return the datiFruizione
	 */
	public String[] getDatiFruizione() {
		return datiFruizione;
	}

}
