package ecg;

import dsl.Query;
import dsl.Sink;

// The detection algorithm (decision rule) that we described in class
// (or your own slight variant of it).
//
// (1) Determine the threshold using the class TrainModel.
//
// (2) When l[n] exceeds the threhold, search for peak (max x[n] or raw[n])
//     in the next 40 samples.
//
// (3) No peak should be detected for 72 samples after the last peak.
//
// OUTPUT: The timestamp of each peak.

public class Detect implements Query<VTL,Long> {

	// Choose this to be two times the average length
	// over the entire signal.
	private static final double THRESHOLD = 255.342056; // 143.914
	private long last_peak;
	private VTL[] records;
	private long counter;
	private int records_counter;
	private boolean on_records;
	// TODO

	public Detect() {
		// TODO
		last_peak = - 100;
		records = new VTL[40];
	}

	@Override
	public void start(Sink<Long> sink) {
		counter = 0;
		records_counter = 0;
		on_records = false;
		// TODO
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		//System.out.println(item.toString());
		counter++;
		if(on_records){
			records_counter ++;
			records[records_counter] = item;
			if(records_counter == 39){
				records_counter=0;
				VTL max_vtl=records[0];
				for(VTL temp: records){
					//System.out.println(temp.toString());
					if(temp.v > max_vtl.v){
						max_vtl=temp;
					}
				}
				//System.out.println(records[0].toString());
				sink.next(max_vtl.ts);
				last_peak=max_vtl.ts;
				on_records=false;
			}
			return;
		}
		if(counter - 72 > last_peak){
			if(item.l>THRESHOLD){
				on_records=true;
				records_counter=0;
				records[records_counter]=item;
				//System.out.println(item.toString());
			}
		}
		return;
	}

	@Override
	public void end(Sink<Long> sink) {
		// TODO
		sink.end();
	}
	
}
