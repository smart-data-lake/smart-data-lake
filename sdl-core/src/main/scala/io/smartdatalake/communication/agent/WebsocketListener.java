package io.smartdatalake.communication.agent;

import com.microsoft.azure.relay.HybridConnectionListener;
import com.microsoft.azure.relay.RelayConnectionStringBuilder;
import com.microsoft.azure.relay.TokenProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class WebsocketListener {
	static boolean quit = false;

	static final String CONNECTION_STRING_ENV_VARIABLE_NAME = "RELAY_CONNECTION_STRING";
	static final RelayConnectionStringBuilder connectionParams = new RelayConnectionStringBuilder(
			"Endpoint=sb://relay-tbb-test.servicebus.windows.net/;EntityPath=relay-tbb-test-connection;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8olAVkveeoQOtErYnjTBEck4bmK9fZc/bkbFTUgjG8U="
			);

	public static void main(String[] args) throws URISyntaxException {
		TokenProvider tokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(
				connectionParams.getSharedAccessKeyName(), connectionParams.getSharedAccessKey());
		HybridConnectionListener listener = new HybridConnectionListener(
				new URI(connectionParams.getEndpoint().toString() + connectionParams.getEntityPath()), tokenProvider);

		listener.openAsync().join();
		System.out.println("Listener is online. Press ENTER to terminate this program.");

	//	while (listener.isOnline()) {

			// If listener closes, then listener.acceptConnectionAsync() will complete with null after closing down
			 listener.acceptConnectionAsync().thenAccept(connection -> {
				// connection may be null if the listener is closed before receiving a connection
				if (connection != null) {
					System.out.println("New session connected.");

					while (connection.isOpen()) {
						ByteBuffer bytesReceived = connection.readAsync().join();
						// If the read operation is still pending when connection closes, the read result as null.
						if (bytesReceived.remaining() > 0) {
							String msg = new String(bytesReceived.array(), bytesReceived.arrayOffset(), bytesReceived.remaining());
							ByteBuffer msgToSend = ByteBuffer.wrap(("Echo: " + msg).getBytes());

							System.out.println("Received: " + msg);
							connection.writeAsync(msgToSend);
						}
					}
					System.out.println("Session disconnected.");
				}
			}).join();
		//}
	}
}
