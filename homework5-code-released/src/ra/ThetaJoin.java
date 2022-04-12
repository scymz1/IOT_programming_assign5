package ra;

import java.util.ArrayList;
import java.util.function.BiPredicate;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

// A streaming implementation of the theta join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the pair (a, b) satisfies a predicate theta.

public class ThetaJoin<A,B> implements Query<Or<A,B>,Pair<A,B>> {

	// TODO
	ArrayList<A> A_records;
	ArrayList<B> B_records;
	BiPredicate<A,B> theta;


	private ThetaJoin(BiPredicate<A,B> theta) {
		this.theta=theta;
		this.A_records = new ArrayList<A>();
		this.B_records = new ArrayList<B>();
		// TODO
	}

	public static <A,B> ThetaJoin<A,B> from(BiPredicate<A,B> theta) {
		return new ThetaJoin<>(theta);
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
				if(theta.test(item.getLeft(), b)){
					sink.next(Pair.from(item.getLeft(), b));
				}
			}
		}
		else{
			this.B_records.add(item.getRight());
			for(A a: this.A_records){
				if(theta.test(a, item.getRight())){
					sink.next(Pair.from(a, item.getRight()));
				}
			}
		}
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		// TODO
		sink.end();
	}
	
}
