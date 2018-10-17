import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import org.neo4j.driver.v1.Record;

public class RecordPublisher implements Publisher<Record>
{
    @Override
    public void subscribe( Subscriber<? super Record> s )
    {

    }
}
