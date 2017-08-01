/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file.schede;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

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
public class UD extends U {
	private String tipologiaUnitaArchivistica = null;
	private String annoIniziale = null;
	private String annoFinale = null;
	private String secoloIniziale = null;
	private String secoloFinale = null;
	private String consistenzaCarte = null;
	private String documentiCartografici = null;
	private Hashtable<String, String> children = null;

	/**
	 * 
	 */
	public UD(Cell[] riga, File fOri, File folderIndex, boolean genMagComp, boolean genMagPub) throws JFileException{
		super(riga, fOri, folderIndex);

		try {

			tipologiaUnitaArchivistica =  ToolsXsl.analizza(riga[16]);
			if (tipologiaUnitaArchivistica ==null){
				throw new JFileException("Asssente la Tipologia Unità Archivistica");
			}

			if (ToolsXsl.analizza(riga[19])!=null){
				annoIniziale = ToolsXsl.analizza(riga[19]).replace(".0", "");
			}

			if (ToolsXsl.analizza(riga[20])!=null){
				annoFinale =  ToolsXsl.analizza(riga[20]).replace(".0", "");
			}

			secoloIniziale =  ToolsXsl.analizza(riga[21]);

			secoloFinale =  ToolsXsl.analizza(riga[22]);

			supporto =  ToolsXsl.analizza(riga[23]);
			if (supporto ==null){
				throw new JFileException("Asssente il Supporto");
			}

			consistenzaCarte =  ToolsXsl.analizza(riga[24]);
			if (consistenzaCarte ==null){
				throw new JFileException("Asssente la Consistenza carte scritte");
			}

			statoConservazione =  ToolsXsl.analizza(riga[25]);
			if (statoConservazione ==null){
				throw new JFileException("Asssente lo Stato di conservazione");
			}

			documentiCartografici =  ToolsXsl.analizza(riga[27]);

			if (ToolsXsl.analizza(riga[2]) != null){
				readChildren(ToolsXsl.analizza(riga[2]), fOri);
			}

			compilatore =  ToolsXsl.analizza(riga[29]);
			if (compilatore ==null){
				throw new JFileException("Asssente il nome del compilatore");
			}

			dataCompilazione =  ToolsXsl.analizza(riga[30]);
			if (dataCompilazione ==null){
				throw new JFileException("Asssente la data compilazione");
			}

			note =  ToolsXsl.analizza(riga[26]);
			
			if (ToolsXsl.analizza(riga[28]) ==null){
				throw new JFileException("Asssente il numero di pagine associate");
			} else {
				if (!ToolsXsl.analizza(riga[28]).equals("")){
					pagine = new Integer(ToolsXsl.analizza(riga[28]).replace(".0", ""));
				}
			}
			magIndex = genMag(fOri, folderIndex, false, genMagComp, genMagPub, pagine, magIndex, bid, titolo, 
					lingua, collocazione, soggettoConservatore, null);

		} catch (NumberFormatException e) {
			throw new JFileException(e.getMessage(), e);
		} catch (JFileException e) {
			throw e;
		}
	}

	private void readChildren(String children, File fOri) throws JFileException{
		Workbook wb = null;
		Sheet finestra = null;
		Cell[] riga = null;
		File f = null;
		String[] st = null;
		
		st = children.split(" ; ");
		this.children = new Hashtable<String, String>();
		for (int x=0; x<st.length; x++){
			try {
				if (!st[x].trim().equals("")){
					f = new File(fOri.getParentFile().getAbsolutePath()+
							File.separator+
							st[x].trim()+".xls");
					if (f.exists()){
						// Apro il file xlsx
						wb = JUC.openFileXls(f.getAbsolutePath());
		
						// Leggo le infomrazioni della 1 finestra
						finestra = wb.getSheet(0);
						riga = finestra.getRow(1);
						this.children.put(st[x].trim(), ToolsXsl.analizza(riga[17]));
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
	}

	/**
	 * @return the tipologiaUnitaArchivistica
	 */
	public String getTipologiaUnitaArchivistica() {
		return tipologiaUnitaArchivistica;
	}

	/**
	 * @return the annoIniziale
	 */
	public String getAnnoIniziale() {
		return annoIniziale;
	}

	/**
	 * @return the annoFinale
	 */
	public String getAnnoFinale() {
		return annoFinale;
	}

	/**
	 * @return the secoloIniziale
	 */
	public String getSecoloIniziale() {
		return secoloIniziale;
	}

	/**
	 * @return the secoloFinale
	 */
	public String getSecoloFinale() {
		return secoloFinale;
	}

	/**
	 * @return the consistenzaCarte
	 */
	public String getConsistenzaCarte() {
		return consistenzaCarte;
	}

	/**
	 * @return the documentiCartografici
	 */
	public String getDocumentiCartografici() {
		return documentiCartografici;
	}

	/**
	 * @return the children
	 */
	public Hashtable<String, String> getChildren() {
		return children;
	}

}
