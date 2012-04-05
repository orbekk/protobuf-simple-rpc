import java.util.concurrent.BlockingQueue;

/**
 * TODO: Move services to this class.
 */

public class RequestHandler extends Thread {
    private volatile boolean isStopped = false;
    private final BlockingQueue<Data.Request> input;
    private final BlockingQueue<Data.Response> output;

    public RequestHandler(BlockingQueue<Data.Request> input,
            BlockingQueue<Data.Response> output) {
        this.input = input;
        this.output = output;
    }

    private void handleRequest() {

    }

    @Override public void run() {
        while (!isStopped) {
            handleRequest();
        }
    }

    @Override public void interrupt() {
        super.interrupt();
        isStopped = true;
    }
}
