/**
 * 
 */
package mx.teca2015.tecaPublished.standAlone.quartz.file.exception;

/**
 * @author massi
 *
 */
public class JFileException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2033124016216067112L;

	/**
	 * @param message
	 */
	public JFileException(String message) {
		super(message);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public JFileException(String message, Throwable cause) {
		super(message, cause);
	}

}
