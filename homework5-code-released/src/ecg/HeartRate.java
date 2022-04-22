package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;

// This file is devoted to the analysis of the heart rate of the patient.
// It is assumed that PeakDetection.qPeaks() has already been implemented.

public class HeartRate {

	// RR interval length (in milliseconds)
	public static Query<Integer,Double> qIntervals() {
		// TODO
		Query<Integer, Double> sub = Q.pipeline(PeakDetection.qPeaks(), Q.sWindow2((x, y) -> (double) y - (double) x));
		Query<Integer, Double> res = Q.pipeline(sub, Q.map(x -> 1000 * x / 360));
		return res;
	}

	// Average heart rate (over entire signal) in bpm.
	public static Query<Integer,Double> qHeartRateAvg() {
		// TODO
		Query<Long, Integer> count = new PeakDetection.counter<>();
		// Query<VTL, Integer> q1 = Q.pipeline(new Detect(), count);
		Query<Integer, Integer> beat = Q.pipeline(PeakDetection.qPeaks(), count);
		Query<Integer, Double> last = Q.fold((double) 0, (x, y) -> (double) y);
		Query<Integer, Double> res = Q.pipeline(beat, last);

		Query<Integer, Double> result = Q.pipeline(res, Q.map(x -> x / ((double) 5000 / 21600)));
		return result;
	}

	// Standard deviation of NN interval length (over the entire signal)
	// in milliseconds.
	public static Query<Integer,Double> qSDNN() {
		// TODO
		Query<Integer, Double> sub = Q.pipeline(PeakDetection.qPeaks(), Q.sWindow2((x, y) -> (double) y - (double) x));
		Query<Integer, Double> res = Q.pipeline(sub, Q.map(x -> 1000 * x / 360));
		Query<Integer, Double> result = Q.pipeline(res, Q.foldStdev());
		return result;
	}

	// RMSSD measure (over the entire signal) in milliseconds.
	public static Query<Integer,Double> qRMSSD() {
		// TODO
		// sum of (Ti+1 - Ti)^2
		Query<Integer, Double> sub = Q.pipeline(PeakDetection.qPeaks(), Q.sWindow2((x, y) -> (double) y - (double) x));
		Query<Integer, Double> mil = Q.pipeline(sub, Q.map(x -> 1000 * x / 360));
		Query<Integer, Double> inter_sub = Q.pipeline(mil, Q.sWindow2((x, y) -> (double) y - (double) x));
		Query<Integer, Double> square = Q.pipeline(inter_sub, Q.map(x -> (double) x * x));
		Query<Double, Double> sum = Q.fold((double) 0, (x, y) -> (double) x + y);
		Query<Integer, Double> value = Q.pipeline(square, sum);

		// number of peaks
		Query<Long, Integer> count = new PeakDetection.counter<>();
		Query<Integer, Integer> beat = Q.pipeline(PeakDetection.qPeaks(), count);
		Query<Integer, Double> add = Q.fold((double) 0, (x, y) -> (double) x + 1);
		Query<Integer, Double> num = Q.pipeline(beat, add);

		// result
		Query<Integer, Double> div = Q.parallel(value, num, (x, y) -> x / y);
		Query<Integer, Double> res = Q.pipeline(div, Q.map(x -> Math.sqrt(x)));
		return res;
	}

	// Proportion (in %) derived by dividing NN50 by the total number
	// of NN intervals (calculated over the entire signal).
	public static Query<Integer,Double> qPNN50() {
		// TODO
		Query<Integer, Double> sub = Q.pipeline(PeakDetection.qPeaks(), Q.sWindow2((x, y) -> (double) y - (double) x));
		Query<Integer, Double> mil = Q.pipeline(sub, Q.map(x -> 1000 * x / 360));
		Query<Integer, Double> inter_sub = Q.pipeline(mil, Q.sWindow2((x, y) -> (double) y - (double) x));
		Query<Double, Double> filter = Q.filter(x -> x >= 50);
		Query<Integer, Double> res = Q.pipeline(inter_sub, filter);

		Query<Double, Double> add = Q.fold((double) 0, (x, y) -> (double) x + 1);
		Query<Integer, Double> num = Q.pipeline(res, add);

		// number of peaks
		Query<Long, Integer> count = new PeakDetection.counter<>();
		Query<Integer, Integer> beat = Q.pipeline(PeakDetection.qPeaks(), count);
		Query<Integer, Double> add_2 = Q.fold((double) 0, (x, y) -> (double) x + 1);
		Query<Integer, Double> totalNN = Q.pipeline(beat, add_2, Q.map(x -> x - 1));
		Query<Integer, Double> result = Q.parallel(num, totalNN, (x, y) -> (double) 100 * x / (double) y);

		return result;
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for the Heart Rate *****");
		System.out.println("****************************************");
		System.out.println();

		System.out.println("***** Intervals *****");
		Q.execute(Data.ecgStream("100.csv"), qIntervals(), S.printer());
		System.out.println();

		System.out.println("***** Average heart rate *****");
		Q.execute(Data.ecgStream("100-all.csv"), qHeartRateAvg(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: SDNN *****");
		Q.execute(Data.ecgStream("100-all.csv"), qSDNN(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: RMSSD *****");
		Q.execute(Data.ecgStream("100-all.csv"), qRMSSD(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: pNN50 *****");
		Q.execute(Data.ecgStream("100-all.csv"), qPNN50(), S.printer());
		System.out.println();
	}

}
