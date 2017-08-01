/**
 * 
 */
package mx.test;

import java.io.File;
import java.io.IOException;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;
import mx.teca2015.tecaPublished.standAlone.quartz.file.xlsx.ToolsXsl;

/**
 * @author massi
 *
 */
public class TestReadXls {

	/**
	 * 
	 */
	public TestReadXls() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Workbook wb = null;
		Sheet finestra = null;
		Cell[] riga = null;

		try {
			// Apro il file xlsx
			wb = openFileXls(args[0]);

			// Leggo le infomrazioni della 1 finestra
			finestra = wb.getSheet(0);
			
			for (int x=1; x<finestra.getRows(); x++){
				riga = finestra.getRow(x);
				for (int y=0; y<riga.length; y++){
					System.out.println("Riga: "+x+" Colonna: "+y+" = "+ToolsXsl.analizza(riga[y]));
				}
			}
		} catch (BiffException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IndexOutOfBoundsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Workbook openFileXls(String fileXls) throws BiffException, IndexOutOfBoundsException, IOException {
		File f = null;
		Workbook wb = null;
		WorkbookSettings wbs = null;

		try {
			f = new File(fileXls);
			if (f.exists()) {
				wbs = new WorkbookSettings();
				wbs.setEncoding("UTF-8");
				wb = Workbook.getWorkbook(f);
			} else {
				System.out.println("Il file [" + f.getAbsolutePath()
						+ "] non esiste");
			}
		} catch (BiffException e) {
			throw e;
		} catch (IndexOutOfBoundsException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		return wb;
	}

}
