/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.san;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * @author massi
 *
 */
public class ComplessoArchivistico {

	private String id = null;
	private String denominazione = null;
	private String tipologia = null;
	private String data = null;
	private String consistenza = null;
	private String consistenzaSast = null;
	private String descrizione = null;
	private String sistemaAderente = null;
	private String schedaProvenienza = null;
	private String soggettoConservatore = null;
	private ComplessoArchivistico complessoArchivisticoMadre = null;
	private Vector<SoggettoProduttore> soggettiProduttori = null;
	private Hashtable<String, ComplessoArchivistico> complessoArchivisticoFigli = null;

	/**
	 * 
	 */
	public ComplessoArchivistico(String[] complessoArchivistico,
			Hashtable<String, String[]> compArch,
			Hashtable<String, Hashtable<String, String[]>> compArchFigli,
			Hashtable<String, String[]> soggettoProduttore) {
		Hashtable<String, String[]> figli = null;
		Enumeration<String> keys = null;
		String key =null;

		id = complessoArchivistico[0];
		denominazione = complessoArchivistico[1];
		tipologia = complessoArchivistico[2];
		data = complessoArchivistico[3];
		consistenza = complessoArchivistico[4];
		consistenzaSast = complessoArchivistico[5];
		descrizione = complessoArchivistico[6];
		sistemaAderente = complessoArchivistico[7];
		schedaProvenienza = complessoArchivistico[8];
		soggettoConservatore = complessoArchivistico[9];

		if (complessoArchivistico.length>10 && 
				!complessoArchivistico[10].trim().equals("") &&
				compArch.get(complessoArchivistico[10].trim())!= null){
			complessoArchivisticoMadre = new ComplessoArchivistico(compArch.get(complessoArchivistico[10].trim()), 
					compArch,null, soggettoProduttore);
		}
		if (complessoArchivistico.length>11){
			for (int x=11; x<complessoArchivistico.length; x++){
				
				if (soggettoProduttore.get(complessoArchivistico[x].trim()) != null){
					if (soggettiProduttori == null){
						soggettiProduttori = new Vector<SoggettoProduttore>();
					}
					soggettiProduttori.add(new SoggettoProduttore(soggettoProduttore.get(complessoArchivistico[x].trim())));
				}
			}
		}

		if (compArchFigli != null && compArchFigli.get(id) != null){
			complessoArchivisticoFigli = new Hashtable<String, ComplessoArchivistico>();
			figli = compArchFigli.get(id);
			keys = figli.keys();
			while(keys.hasMoreElements()){
				key = keys.nextElement();
				complessoArchivisticoFigli.put(key, 
						new ComplessoArchivistico(figli.get(key), compArch, compArchFigli, soggettoProduttore));
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
	 * @return the tipologia
	 */
	public String getTipologia() {
		return tipologia;
	}

	/**
	 * @return the data
	 */
	public String getData() {
		return data;
	}

	/**
	 * @return the consistenza
	 */
	public String getConsistenza() {
		return consistenza;
	}

	/**
	 * @return the descrizione
	 */
	public String getDescrizione() {
		return descrizione;
	}

	/**
	 * @return the sistemaAderente
	 */
	public String getSistemaAderente() {
		return sistemaAderente;
	}

	/**
	 * @return the schedaProvenienza
	 */
	public String getSchedaProvenienza() {
		return schedaProvenienza;
	}

	/**
	 * @return the soggettoConservatore
	 */
	public String getSoggettoConservatore() {
		return soggettoConservatore;
	}

	/**
	 * @return the complessoArchivisticoMadre
	 */
	public ComplessoArchivistico getComplessoArchivisticoMadre() {
		return complessoArchivisticoMadre;
	}

	/**
	 * @return the soggettiProduttori
	 */
	public Vector<SoggettoProduttore> getSoggettiProduttori() {
		return soggettiProduttori;
	}

	/**
	 * @return the consistenzaSast
	 */
	public String getConsistenzaSast() {
		return consistenzaSast;
	}

	/**
	 * @return the complessoArchivisticoFigli
	 */
	public Hashtable<String, ComplessoArchivistico> getComplessoArchivisticoFigli() {
		return complessoArchivisticoFigli;
	}

}
