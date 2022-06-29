package it.unipd.dei.ims.datastructure;

/** My personal implementation of a simple RDF node,
 * with only a string and the radius position
 * from a center. No big deal.
 * 
 * */
public class RDFSimpleNode {

	private String uri;

	private int radius;

	public RDFSimpleNode () {

	}

	public RDFSimpleNode (String u, int radius) {
		this.uri = u;
		this.radius = radius;
	}


	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public int getRadius() {
		return radius;
	}

	public void setRadius(int radius) {
		this.radius = radius;
	}

}
