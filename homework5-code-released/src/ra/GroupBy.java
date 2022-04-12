package ra;

import java.util.ArrayList;

import dsl.Query;
import dsl.Sink;
import utils.Pair;
import utils.functions.Func2;

// A streaming implementation of the "group by" (and aggregate) operator.
//
// The input consists of one channel that carries key-value pairs of the
// form (k, a) where k \in K and a \in A.
// For every key k, we perform a separate aggregation in the style of fold.
// When the input stream ends, we output all results (k, b), where k is
// a key and b is the aggregate for k.
//
// The keys in the output should be given in the order of their first occurrence
// in the input stream. That is, if k1 occurred earlier than k2 in the input
// stream, then the output (k1, b1) should be given before (k2, b2) in the
// output.

public class GroupBy<K,A,B> implements Query<Pair<K,A>,Pair<K,B>> {

	// TODO
	B init;
	Func2<B,A,B> op;
	ArrayList<Pair<K, B>> records;

	private GroupBy(B init, Func2<B,A,B> op) {
		// TODO
		this.init=init;
		this.op=op;
		this.records = new ArrayList<Pair<K, B>>();
	}

	public static <K,A,B> GroupBy<K,A,B> from(B init, Func2<B,A,B> op) {
		return new GroupBy<>(init, op);
	}

	@Override
	public void start(Sink<Pair<K,B>> sink) {
		// TODO
	}

	@Override
	public void next(Pair<K,A> item, Sink<Pair<K,B>> sink) {
		// TODO
		K key=item.getLeft();
		Boolean flag = false;
		for(int i = 0; i < records.size(); i++){
			Pair<K, B> p=records.get(i);
			if(p.getLeft() == key){
				flag=true;
				B result=op.apply(p.getRight(), item.getRight());
				records.set(i, Pair.from(key, result));
				break;
			}
		}
		if(!flag){
			B result=op.apply(init, item.getRight());
			records.add(Pair.from(item.getLeft(), result));
		}
	}

	@Override
	public void end(Sink<Pair<K,B>> sink) {
		// TODO
		for(Pair<K, B> p: records){
			sink.next(p);
		}
	}
	
}
