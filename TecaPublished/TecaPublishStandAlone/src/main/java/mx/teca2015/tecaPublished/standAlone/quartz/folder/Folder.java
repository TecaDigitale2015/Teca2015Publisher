/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.folder;

import java.io.Serializable;

/**
 * @author massi
 *
 */
public class Folder implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5570499216491143948L;

	private String name = null;
	
	private String path = null;
	
	private String pathIndex = null;

	private String fondo =  null;

	private String tipoRisorsa =  null;

	/**
	 * 
	 */
	public Folder() {
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @return the pathIndex
	 */
	public String getPathIndex() {
		return pathIndex;
	}

	/**
	 * @param pathIndex the pathIndex to set
	 */
	public void setPathIndex(String pathIndex) {
		this.pathIndex = pathIndex;
	}

	/**
	 * @return the fondo
	 */
	public String getFondo() {
		return fondo;
	}

	/**
	 * @param fondo the fondo to set
	 */
	public void setFondo(String fondo) {
		this.fondo = fondo;
	}

	/**
	 * @return the tipoRisorsa
	 */
	public String getTipoRisorsa() {
		return tipoRisorsa;
	}

	/**
	 * @param tipoRisorsa the tipoRisorsa to set
	 */
	public void setTipoRisorsa(String tipoRisorsa) {
		this.tipoRisorsa = tipoRisorsa;
	}

}
