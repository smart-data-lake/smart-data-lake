package io.smartdatalake.communication.agent;

import com.microsoft.azure.relay.HybridConnectionClient;
import com.microsoft.azure.relay.RelayConnectionStringBuilder;
import com.microsoft.azure.relay.TokenProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class WebsocketSender {

	static final RelayConnectionStringBuilder connectionParams = new RelayConnectionStringBuilder(
			"Endpoint=sb://relay-tbb-test.servicebus.windows.net/;EntityPath=relay-tbb-test-connection;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=x9/63Y8JRqsNX0ad0xeJQzLUU5xpa9E5f9RxhQS3YcI="
	);

	public static void main(String[] args) throws InterruptedException, ExecutionException, URISyntaxException {
		TokenProvider tokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(
				connectionParams.getSharedAccessKeyName(),
				connectionParams.getSharedAccessKey());
		HybridConnectionClient client = new HybridConnectionClient(
				new URI(connectionParams.getEndpoint().toString() + connectionParams.getEntityPath()),
				tokenProvider);

		Scanner in = new Scanner(System.in);

		client.createConnectionAsync().thenAccept((connection) -> {
			while (connection.isOpen()) {
				System.out.println("Please enter the text you want to send, or enter \"quit\" or \"q\" to exit");
				String input = in.nextLine();
				if (input.equalsIgnoreCase("quit") || input.equalsIgnoreCase("q"))
					break;

				connection.writeAsync(ByteBuffer.wrap(input.getBytes())).thenRun(() -> {
					connection.readAsync().thenAccept((byteBuffer) -> {
						// If the read operation is still pending when connection closes, the read result returns null.
						if (byteBuffer != null) {
							System.out.println("Received: " + new String(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.remaining()));
						}
					});
				}).join();
			}
			connection.closeAsync().join();
		}).whenComplete((result, exception) -> {
			if (exception != null) {
				exception.printStackTrace();
			}
			in.close();
		});
	}
}
