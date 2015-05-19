package de.jungblut.clustering.model;
public class DistanceMeasurer {

	/**
	 * Manhattan stuffz.
	 * 
	 * @param center
	 * @param v
	 * @return
	 */
	private static final int NUM_COLUMNS = 4;
	public static final double measureDistance(ClusterCenter center, Vector v) {
		double sum = 0;
		int length = Math.min(center.getCenter().vector.length, v.getVector().length);
		for (int i = 0; i < length; i++) {
			for(int j = 0; j < NUM_COLUMNS; j++)
			     sum += Math.abs(center.getCenter().getVector()[i].data[j]- v.getVector()[i].data[j]);		
		}

		return sum;
	}
	public static double measureDistance(Vector c, Vector v) {
		double sum = 0;
		int length = v.getVector().length;
		for (int i = 0; i < length; i++) {
			for(int j = 0; j < NUM_COLUMNS; j++)
			     sum += Math.abs(c.getVector()[i].data[j]- v.getVector()[i].data[j]);
		}

		return sum;
	}
}
