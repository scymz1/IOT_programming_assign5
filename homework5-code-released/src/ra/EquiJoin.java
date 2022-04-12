package ra;

import java.util.ArrayList;
import java.util.function.Function;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

// A streaming implementation of the equijoin operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the equality predicate f(a) = g(b) holds.

public class EquiJoin<A,B,T> implements Query<Or<A,B>,Pair<A,B>> {

	Function<A,T> f;
	Function<B,T> g;
	ArrayList<A> A_records;
	ArrayList<B> B_records;

	private EquiJoin(Function<A,T> f, Function<B,T> g) {
		this.f=f;
		this.g=g;
		this.A_records = new ArrayList<A>();
		this.B_records = new ArrayList<B>();
	}

	public static <A,B,T> EquiJoin<A,B,T> from(Function<A,T> f, Function<B,T> g) {
		return new EquiJoin<>(f, g);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		// TODO
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		// TODO
		if(item.isLeft()){
			Integer temp= (Integer) item.getLeft();
			System.out.println(temp);
			this.A_records.add(item.getLeft());
			for(B b: B_records){
				if(f.apply(item.getLeft()).equals(g.apply(b))){
					sink.next(Pair.from(item.getLeft(), b));
				}
			}
		}
		else{
			this.B_records.add(item.getRight());
			for(A a: this.A_records){
				if(f.apply(a).equals(g.apply(item.getRight()))){
					sink.next(Pair.from(a, item.getRight()));
				}
			}
		}
		/**
		if(A_records.size()>0 && B_records.size()>0){
			System.out.println(A_records.get(A_records.size()-1));
			System.out.println(B_records.get(B_records.size()-1));

		}
		 */
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		// TODO
		sink.end();
	}
	
}
