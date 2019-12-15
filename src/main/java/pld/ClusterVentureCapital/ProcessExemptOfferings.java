package pld.ClusterVentureCapital;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import pld.IngestExemptOfferings.ExemptOffering;

public abstract class ProcessExemptOfferings {

    /**
     * Process exempt offering filings on SEC form D for a Hive application.
     * Implementation should override the serializeFormD method to incorporate 
     * HashMap and SequenceFile objects.
     */

    abstract void serializeFormD(ExemptOffering exemptOfferingData, Integer year) throws IOException;

    void readDirectory(final String exemptOfferingsDirectory) throws IOException, ParserConfigurationException {

        /**
         * Read a directory of exempt offering filings on SEC form D and send to
         * serialize. Assume only XML files exist in the given directory.
         * 
         * exemptOfferingsDirectory (String): relative path to the directory.
         */

        // Initialize a DocumentBuilder object with which to read XML files.
        final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        final DocumentBuilder interpreter = dbf.newDocumentBuilder();

        // Set up the interpreter with error-handling logic.
        final String outputEncoding = "UTF-8";
        final OutputStreamWriter errorWriter = new OutputStreamWriter(System.err, outputEncoding);
        interpreter.setErrorHandler(new WeakErrorHandler (new PrintWriter(errorWriter, true)));

        // Read the directory of exempt offering filings.
        final File[] exemptOfferings = new File(exemptOfferingsDirectory).listFiles();
        for (File exemptOffering : exemptOfferings) readFormD(exemptOffering, interpreter);
    }

    void readFormD(final File exemptOffering, final DocumentBuilder interpreter) throws IOException {

        /**
         * Read an exempt offering filing on SEC form D and send to serialize.
         * 
         * exemptOffering (File): XML file.
         * interpreter (DocumentBuilder): XML format interpreter.
         */

        try {
            // Parse the XML filing.
            final Document formD = interpreter.parse(exemptOffering);
            formD.getDocumentElement().normalize();

            // Collect the required data.
            Integer cik = Integer.parseInt(formD.getElementsByTagName("cik").item(0).getTextContent().trim());
            String entity = formD.getElementsByTagName("entityName").item(0).getTextContent().trim();
            String date = formD.getElementsByTagName("signatureDate").item(0).getTextContent().trim();
            Short year = Short.parseShort(date.substring(0, 4));
            Byte month = Byte.parseByte(date.substring(5, 7));
            Byte day = Byte.parseByte(date.substring(8, 10));
            Short census = (short) (year - (year % 10));
            String zip = formD.getElementsByTagName("zipCode").item(0).getTextContent().trim();
            String industry = formD.getElementsByTagName("industryGroupType").item(0).getTextContent().trim();
            Float offering = Float.parseFloat(formD.getElementsByTagName("totalAmountSold").item(0).getTextContent().trim());
            
            // Read data into the struct.
            ExemptOffering exemptOfferingData = new ExemptOffering(
                cik, entity, census, year, month, day, zip, industry, offering
            );

            // Serialize the data.
            serializeFormD(exemptOfferingData, year.intValue());

        // Skip malformed XML files and catch all other exceptions..
        } catch (SAXParseException e) {
            // e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class WeakErrorHandler implements ErrorHandler {

    /**
     * Weakly handle errors for document builder, which must report SAX
     * exceptions when it has trouble parsing an XML document.
     */
     
    private final PrintWriter out;

    WeakErrorHandler(final PrintWriter out) {
        this.out = out;
    }

    private String getParseExceptionInfo(final SAXParseException spe) {
        String systemId = spe.getSystemId();
        if (systemId == null) {
            systemId = "null";
        }
        final String info = "URI=" + systemId + " Line=" + spe.getLineNumber() +
                      ": " + spe.getMessage();
        return info;
    }

    public void warning(final SAXParseException spe) throws SAXException {
        out.println("Warning: " + getParseExceptionInfo(spe));
    }
        
    public void error(final SAXParseException spe) throws SAXException {
        final String message = "Error: " + getParseExceptionInfo(spe);
        // throw new SAXException(message);
    }

    public void fatalError(final SAXParseException spe) throws SAXException {
        final String message = "Fatal Error: " + getParseExceptionInfo(spe);
        // throw new SAXException(message);
    }
}