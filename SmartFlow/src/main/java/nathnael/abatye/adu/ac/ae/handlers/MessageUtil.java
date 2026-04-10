package nathnael.abatye.adu.ac.ae.handlers;




import nathnael.abatye.adu.ac.ae.EventMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;


public final class MessageUtil {

    private MessageUtil() {}

    public static EventMessage readAll(InputStream input) throws IOException {
        // Wrap input stream so we can read text line by line (UTF-8 encoding)
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(input, StandardCharsets.UTF_8)
        );

        StringBuilder sb = new StringBuilder(); // Used to build the full message
        String line;
        boolean first = true; // Helps avoid adding extra newline at the start

        // Read input line by line until no more data
        while ((line = reader.readLine()) != null) {
            if (!first) {
                sb.append('\n'); // Add newline between lines (not before first)
            }
            sb.append(line); // Add current line to result
            first = false;
        }

        String JsonEventMessage = sb.toString();
       return EventMessage.fromJson(JsonEventMessage) ;
    
    }


    public static void writeText(OutputStream output, String text) throws IOException {
        // Create writer to send text using UTF-8 encoding
        PrintWriter writer = new PrintWriter(output, false);
        writer.println(text); // Send the text (adds newline at the end)
        writer.flush();       // Make sure data is actually sent
        output.close();       // Close the connection/output stream
    }

    public static String classifyMessage(String message) {
        // Check message content and classify based on keywords

        if (message.contains("STATE:")) {
            return "Traffic Light Status";   // Message about traffic light state
        } else if (message.contains("CARS_DETECTED:")) {
            return "Traffic Sensor Data";   // Message from sensors detecting cars
        } else if (message.contains("EMERGENCY:")) {
            return "Emergency Alert";       // Message indicating emergency
        }

        return "Unknown Message Type"; // If no known pattern is found
    }
}
