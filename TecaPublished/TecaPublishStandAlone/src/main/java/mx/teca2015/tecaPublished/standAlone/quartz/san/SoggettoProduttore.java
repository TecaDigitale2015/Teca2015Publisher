/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.san;

/**
 * @author massi
 *
 */
public class SoggettoProduttore {
	private String id = null;
	private String tipologia = null;
	private String formaAutorizzata = null;
	private String altreDenominazioni = null;
	private String dataEsistenza = null;
	private String dataMorte = null;
	private String luogoNascita = null;
	private String luogoMorte = null;
	private String sede = null;
	private String naturaGiuridica = null;
	private String tipoEnte = null;
	private String ambitoTerritoriale = null;
	private String titolo = null;
	private String descrizione = null;
	private String sistemaAderente = null;
	private String schedaProvenienza = null;

	/**
	 * 
	 */
	public SoggettoProduttore(String[] soggettoProduttore) {
		id = soggettoProduttore[0];
		tipologia = soggettoProduttore[1];
		formaAutorizzata = soggettoProduttore[2];
		altreDenominazioni = soggettoProduttore[3];
		dataEsistenza = soggettoProduttore[4];
		dataMorte = soggettoProduttore[5];
		luogoNascita = soggettoProduttore[6];
		luogoMorte = soggettoProduttore[7];
		sede = soggettoProduttore[8];

		if (soggettoProduttore.length>10){
			naturaGiuridica = soggettoProduttore[9];
		}

		if (soggettoProduttore.length>11){
			tipoEnte = soggettoProduttore[10];
		}

		if (soggettoProduttore.length>12){
			ambitoTerritoriale = soggettoProduttore[11];
		}

		if (soggettoProduttore.length>13){
			titolo = soggettoProduttore[12];
		}

		if (soggettoProduttore.length>14){
			descrizione = soggettoProduttore[13];
		}

		if (soggettoProduttore.length>15){
			sistemaAderente = soggettoProduttore[14];
		}

		if (soggettoProduttore.length>16){
			schedaProvenienza = soggettoProduttore[15];
		}
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the tipologia
	 */
	public String getTipologia() {
		return tipologia;
	}

	/**
	 * @return the formaAutorizzata
	 */
	public String getFormaAutorizzata() {
		return formaAutorizzata;
	}

	/**
	 * @return the altreDenominazioni
	 */
	public String getAltreDenominazioni() {
		return altreDenominazioni;
	}

	/**
	 * @return the dataEsistenza
	 */
	public String getDataEsistenza() {
		return dataEsistenza;
	}

	/**
	 * @return the dataMorte
	 */
	public String getDataMorte() {
		return dataMorte;
	}

	/**
	 * @return the luogoNascita
	 */
	public String getLuogoNascita() {
		return luogoNascita;
	}

	/**
	 * @return the luogoMorte
	 */
	public String getLuogoMorte() {
		return luogoMorte;
	}

	/**
	 * @return the sede
	 */
	public String getSede() {
		return sede;
	}

	/**
	 * @return the naturaGiuridica
	 */
	public String getNaturaGiuridica() {
		return naturaGiuridica;
	}

	/**
	 * @return the tipoEnte
	 */
	public String getTipoEnte() {
		return tipoEnte;
	}

	/**
	 * @return the ambitoTerritoriale
	 */
	public String getAmbitoTerritoriale() {
		return ambitoTerritoriale;
	}

	/**
	 * @return the titolo
	 */
	public String getTitolo() {
		return titolo;
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

}
