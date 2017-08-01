/**
 * 
 */
package mx.test;

import java.io.File;

import mx.randalf.schedaF.CsmRoot;
import mx.randalf.schedaF.CsmRootXsd;
import mx.randalf.xsd.exception.XsdException;

/**
 * @author massi
 *
 */
public class SchedaFTest {

	/**
	 * 
	 */
	public SchedaFTest() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CsmRootXsd csmRootXsd = null;
		CsmRoot csmRoot = null;
		
		try {
			csmRootXsd = new CsmRootXsd();
			csmRoot = csmRootXsd.read(new File(args[0]));
			System.out.println(csmRoot.getCsmInfo().getEnteSchedatore());
			for (int x=0; x<csmRoot.getSchede().getScheda().size(); x++){
				System.out.println("Scheda "+x);
				System.out.println(csmRoot.getSchede().getScheda().get(x).getCD().getNCT().getNCTR().getValue()+
						csmRoot.getSchede().getScheda().get(x).getCD().getNCT().getNCTN().getValue());
			}
		} catch (XsdException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
