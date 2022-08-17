package net.plumbing.msgbus.ws.legacy;

import net.plumbing.msgbus.ws.SoapBuilderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.xmlbeans.*;
import net.plumbing.msgbus.ws.common.ResourceUtils;
import org.w3.x2003.x05.soapEnvelope.EnvelopeDocument;
import org.w3.x2003.x05.soapEnvelope.FaultDocument;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.net.URL;

/**
 * This class was extracted from the soapUI code base by centeractive ag in October 2011.
 * The main reason behind the extraction was to separate the code that is responsible
 * for the generation of the SOAP messages from the rest of the soapUI's code that is
 * tightly coupled with other modules, such as soapUI's graphical user interface, etc.
 * The goal was to create an open-source java project whose main responsibility is to
 * handle SOAP message generation and SOAP transmission purely on an XML level.
 * <br/>
 * centeractive ag would like to express strong appreciation to SmartBear Software and
 * to the whole team of soapUI's developers for creating soapUI and for releasing its
 * source code under a free and open-source licence. centeractive ag extracted and
 * modifies some parts of the soapUI's code in good faith, making every effort not
 * to impair any existing functionality and to supplement it according to our
 * requirements, applying best practices of software design.
 *
 * Changes done:
 * - changing location in the package structure
 * - removal of dependencies and code parts that are out of scope of SOAP message generation
 * - minor fixes to make the class compile out of soapUI's code base
 * - changing the mechanism using which external resources are loaded
 */

/**
 * SoapVersion for SOAP 1.2
 *
 * @author ole.matzura
 */
class SoapVersion12 extends AbstractSoapVersion {

    private final static QName envelopeQName = new QName(Constants.SOAP12_ENVELOPE_NS, "Envelope");
    private final static QName bodyQName = new QName(Constants.SOAP12_ENVELOPE_NS, "Body");
    private final static QName faultQName = new QName(Constants.SOAP11_ENVELOPE_NS, "Fault");
    private final static QName headerQName = new QName(Constants.SOAP12_ENVELOPE_NS, "Header");
    public final static SoapVersion12 instance = new SoapVersion12();

    private SchemaTypeLoader soapSchema;
    private XmlObject soapSchemaXml;
    private XmlObject soapEncodingXml;

    private SoapVersion12() {

        try {
            URL soapSchemaXmlResource = ResourceUtils.getResourceWithAbsolutePackagePath(getClass(),
                    "/xsds/", "soapEnvelope12.xsd");
            soapSchemaXml = XmlUtils.createXmlObject(soapSchemaXmlResource);
            soapSchema = XmlBeans.loadXsd(new XmlObject[]{soapSchemaXml});

            URL soapEncodingXmlResource = ResourceUtils.getResourceWithAbsolutePackagePath(getClass(),
                    "/xsds/", "soapEncoding12.xsd");
            soapEncodingXml = XmlUtils.createXmlObject(soapEncodingXmlResource);
        } catch (XmlException e) {
            throw new SoapBuilderException(e);
        }
    }

    public String getEncodingNamespace() {
        return "http://www.w3.org/2003/05/soap-encoding";
    }

    public XmlObject getSoapEncodingSchema() throws XmlException, IOException {
        return soapEncodingXml;
    }

    public XmlObject getSoapEnvelopeSchema() throws XmlException, IOException {
        return soapSchemaXml;
    }

    public String getEnvelopeNamespace() {
        return Constants.SOAP12_ENVELOPE_NS;
    }

    public SchemaType getEnvelopeType() {
        return EnvelopeDocument.type;
    }

    public String toString() {
        return "SOAP 1.2";
    }

    public static String quote(String str) {
        if (str == null)
            return str;

        if (str.length() < 2 || !str.startsWith("\"") || !str.endsWith("\""))
            str = "\"" + str + "\"";

        return str;
    }

    public String getContentTypeHttpHeader(String encoding, String soapAction) {
        String result = getContentType();

        if (encoding != null && encoding.trim().length() > 0)
            result += ";charset=" + encoding;

        if (StringUtils.isNotBlank(soapAction))
            result += ";action=" + quote(soapAction);

        return result;
    }

    public String getSoapActionHeader(String soapAction) {
        // SOAP 1.2 has this in the contenttype
        return null;
    }

    public String getContentType() {
        return "application/soap+xml";
    }

    public QName getBodyQName() {
        return bodyQName;
    }

    public QName getEnvelopeQName() {
        return envelopeQName;
    }

    public QName getHeaderQName() {
        return headerQName;
    }

    protected SchemaTypeLoader getSoapEnvelopeSchemaLoader() {
        return soapSchema;
    }

    public static QName getFaultQName() {
        return faultQName;
    }

    public SchemaType getFaultType() {
        return FaultDocument.type;
    }

    public String getName() {
        return "SOAP 1.2";
    }

    public String getFaultDetailNamespace() {
        return getEnvelopeNamespace();
    }
}

