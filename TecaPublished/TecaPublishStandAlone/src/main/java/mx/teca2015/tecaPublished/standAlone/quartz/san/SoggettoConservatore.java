/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.san;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;

import mx.randalf.configuration.exception.ConfigurationException;

/**
 * @author massi
 *
 */
public class SoggettoConservatore {
	private String id = null;
	private String denominazione = null;
	private String tipoSoggettoConservatore =  null;
	private String descrizione = null;
	private String indirizzo = null;
	private String telefono = null;
	private String celllulare = null;
	private String email = null;
	private String servizioConsultazioneAlPubblico = null;
	private String orarioApertura = null;
	private Vector<Dati> schedeConservatori = null;
	private Vector<Dati> risorseEsterne = null;
	private Vector<ComplessoArchivistico> complessiArchivistici =  null;

	/**
	 * @throws IOException 
	 * @throws ConfigurationException 
	 * @throws FileNotFoundException 
	 * 
	 */
	public SoggettoConservatore(String[] testo,
			Hashtable<String, Vector<String[]>> complessiArchivistici,
			Hashtable<String, String[]> compArch, 
			Hashtable<String, Hashtable<String, String[]>> compArchFigli,
			Hashtable<String, String[]> soggettoProduttore) throws FileNotFoundException, ConfigurationException, IOException {
		String[] st = null;

		id = testo[0];
		denominazione = testo[1];
		tipoSoggettoConservatore =  testo[2];
		descrizione = testo[3];
		indirizzo = testo[4];
		telefono = testo[5];
		celllulare = testo[6];
		email = testo[7];
		servizioConsultazioneAlPubblico = testo[8];
		orarioApertura = testo[9];

		if (testo.length>10){
			st = testo[10].split(" ; ");
			schedeConservatori = new Vector<Dati>();
			for (int x=0;x<st.length; x++){
				schedeConservatori.add(new Dati(st[x]));
			}
		}

		if (testo.length>11){
			st = testo[11].split(" ; ");
			risorseEsterne = new Vector<Dati>();
			for (int x=0;x<st.length; x++){
				risorseEsterne.add(new Dati(st[x]));
			}
		}
		init(complessiArchivistici, compArch, compArchFigli, soggettoProduttore);
	}

	private void init(Hashtable<String, Vector<String[]>> complessiArchivistici,
			Hashtable<String, String[]> compArch,
			Hashtable<String, Hashtable<String, String[]>> compArchFigli,
			Hashtable<String, String[]> soggettoProduttore){
		Vector<String[]> ca = null;
		
		if (complessiArchivistici.get(id) != null){
			ca = complessiArchivistici.get(id);
			for (int x=0;x< ca.size(); x++){
				if (this.complessiArchivistici == null){
					this.complessiArchivistici = new Vector<ComplessoArchivistico>();
				}
				this.complessiArchivistici.add(new ComplessoArchivistico(ca.get(x), compArch, compArchFigli, soggettoProduttore));
			}
		}
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the denominazione
	 */
	public String getDenominazione() {
		return denominazione;
	}

	/**
	 * @return the tipoSoggettoConservatore
	 */
	public String getTipoSoggettoConservatore() {
		return tipoSoggettoConservatore;
	}

	/**
	 * @return the descrizione
	 */
	public String getDescrizione() {
		return descrizione;
	}

	/**
	 * @return the indirizzo
	 */
	public String getIndirizzo() {
		return indirizzo;
	}

	/**
	 * @return the telefono
	 */
	public String getTelefono() {
		return telefono;
	}

	/**
	 * @return the celllulare
	 */
	public String getCelllulare() {
		return celllulare;
	}

	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @return the servizioConsultazioneAlPubblico
	 */
	public String getServizioConsultazioneAlPubblico() {
		return servizioConsultazioneAlPubblico;
	}

	/**
	 * @return the orarioApertura
	 */
	public String getOrarioApertura() {
		return orarioApertura;
	}

	/**
	 * @return the schedeConservatori
	 */
	public Vector<Dati> getSchedeConservatori() {
		return schedeConservatori;
	}

	/**
	 * @return the risorseEsterne
	 */
	public Vector<Dati> getRisorseEsterne() {
		return risorseEsterne;
	}

	/**
	 * @return the complessiArchivistici
	 */
	public Vector<ComplessoArchivistico> getComplessiArchivistici() {
		return complessiArchivistici;
	}

}

class Dati {
	private String testo = null;
	private String url = null;

	public Dati(String testo){
		int pos = 0;
		
		pos = testo.indexOf("(");
		if (pos > -1){
			this.testo = testo.substring(0, pos-1).trim();
			this.url = testo.substring(pos+1, testo.length()-1).trim();
		} else {
			this.testo = testo;
		}
	}

	/**
	 * @return the testo
	 */
	public String getTesto() {
		return testo;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
}