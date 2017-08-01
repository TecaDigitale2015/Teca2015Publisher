/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file.xlsx;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
//import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import jxl.Cell;
import jxl.CellType;
import jxl.DateCell;
import jxl.LabelCell;
import jxl.NumberCell;

/**
 * @author massi
 *
 */
public class ToolsXsl {

	/**
	 * 
	 */
	public ToolsXsl() {
	}
	
	public static String analizza(Cell cella) {
		String ris = null;
		LabelCell label = null;
		DateCell date = null;
		NumberCell number = null;
		GregorianCalendar gc = null;
		String testo = "";
		DecimalFormat df2 = new DecimalFormat("00");
		DecimalFormat df4 = new DecimalFormat("0000");

		if (cella.getType() == CellType.LABEL) {
			label = (LabelCell) cella;
			try {
				testo = label.getString();
				if (checkCharSet(testo, "US-ASCII")){
					ris = testo;
				} else if (checkCharSet(testo, "ISO-8859-1")) {
					ris = testo;
				} else if (checkCharSet(testo, "UTF-8")) {
					ris = new String(testo.getBytes("UTF-8"),"ISO-8859-1");
				} else if (checkCharSet(testo, "UTF-16BE")) {
					ris = new String(testo.getBytes("UTF-16BE"),"ISO-8859-1");
				} else if (checkCharSet(testo, "UTF-16LE")) {
					ris = new String(testo.getBytes("UTF-16LE"),"ISO-8859-1");
				} else if (checkCharSet(testo, "UTF-16")) {
					ris = new String(testo.getBytes("UTF-16"),"ISO-8859-1");
				} else {
					ris = new String(testo.getBytes("UTF-8"),"ISO-8859-1");
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (cella.getType() == CellType.DATE) {
			date = (DateCell) cella;
			gc = new GregorianCalendar();
			gc.setTimeInMillis(date.getDate().getTime());
			ris = df2.format(gc.get(Calendar.DAY_OF_MONTH)) + "/"
					+ df2.format((gc.get(Calendar.MONTH) + 1)) + "/"
					+ df4.format(gc.get(Calendar.YEAR));
		} else if (cella.getType() == CellType.NUMBER) {
			number = (NumberCell) cella;
			ris = Double.valueOf(number.getValue()).toString();
		} else if (cella.getType() != CellType.EMPTY) {
			System.out.println("Formato non gestito " + cella.getType());
		}

		return ris;
	}

	@SuppressWarnings("unused")
	private static boolean checkCharSet(String testo, String charSet){
		boolean ris = true;
		CharsetDecoder decoder = null;
		CharBuffer buffer = null;

		decoder = Charset.forName(charSet).newDecoder();
		decoder.onMalformedInput(CodingErrorAction.REPORT);  
	    decoder.onUnmappableCharacter(CodingErrorAction.REPORT); 
	    try {
			buffer = decoder.decode(ByteBuffer.wrap(testo.getBytes()));
			ris = true;
		} catch (CharacterCodingException e) {
			ris = false;
		}
		return ris;
	}

}
