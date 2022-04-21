package ecg;
import java.lang.Math;

import dsl.S;
import dsl.Sink;
import dsl.Q;
import dsl.Query;

public class PeakDetection {

	// The curve length transformation:
	//
	// adjust: x[n] = raw[n] - 1024
	// smooth: y[n] = (x[n-2] + x[n-1] + x[n] + x[n+1] + x[n+2]) / 5
	// deriv: d[n] = (y[n+1] - y[n-1]) / 2
	// length: l[n] = t(d[n-w]) + ... + t(d[n+w]), where
	//         w = 20 (samples) and t(d) = sqrt(1.0 + d * d)

	private static class counter<A> implements Query<A,Integer> {
		private int count;
		public counter() {
			count=0;
		}
	
		@Override
		public void start(Sink<Integer> sink) {
		}
	
		@Override
		public void next(A item, Sink<Integer> sink) {
			sink.next(count);
			count++;
		}
	
		@Override
		public void end(Sink<Integer> sink) {
			sink.end();
		}
	}

	public static Query<Integer,Double> qLength() {
		// adjust >> smooth >> deriv >> length
		Query<Integer,Integer> adjust = Q.map(x -> x - 1024);
		Query<Integer,Double> smooth=Q.pipeline(Q.sWindowNaive(5, 0, (x,y)->x+y), Q.map(x->(double)x/5));
		
		// calculate derivation
		//Q.ignore(2)
		Query<Double,Double> q1= Q.parallel(Q.sWindow3((x, y, z)->z), Q.sWindow3((x, y, z)->x), (x, y)->x-y);
		Query<Double, Double> deriv=Q.pipeline(q1, Q.map(x->x/2));

		// calculate distance
		Query<Double, Double> t=Q.map(x->Math.sqrt(1+x*x));

		Query<Integer,Double> intermediate = Q.pipeline(adjust, smooth, deriv, t);
		Query<Double, Double> sum_dis = Q.sWindowNaive(41, 0.0, (x,y)->x+y);
		Query<Integer,Double> q_final = Q.pipeline(intermediate, sum_dis);

		//TODO
		return q_final;
	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.

	public static Query<Integer,Long> qPeaks() {
		// TODO
		Query<Integer,Double> query_l = PeakDetection.qLength();
		Query<Integer,VT> q1 = Q.parallel(Q.id(), new PeakDetection.counter<>(), (x, y)->new VT(x, y));
		Query<Integer,VT> q2 = Q.pipeline(q1, Q.ignore(23));
		Query<Integer, VTL> q3 = Q.parallel(query_l, q2, (x, y)->y.extendl(x));
		Query<Integer, Long> result = Q.pipeline(q3, new Detect());
		return result;
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100-samples-500.csv"), qPeaks(), S.printer());
	}

}
