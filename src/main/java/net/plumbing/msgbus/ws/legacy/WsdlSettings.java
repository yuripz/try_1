package net.plumbing.msgbus.ws.legacy;

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
 */

/**
 * WSDL related settings constants
 *
 * @author Emil.Breding
 */
interface WsdlSettings {
    public final static String CACHE_WSDLS = WsdlSettings.class.getSimpleName() + "@" + "cache-wsdls";

    public final static String XML_GENERATION_TYPE_EXAMPLE_VALUE = WsdlSettings.class.getSimpleName() + "@"
            + "xml-generation-type-example-value";

    public final static String XML_GENERATION_TYPE_COMMENT_TYPE = WsdlSettings.class.getSimpleName() + "@"
            + "xml-generation-type-comment-type";

    public final static String XML_GENERATION_ALWAYS_INCLUDE_OPTIONAL_ELEMENTS = WsdlSettings.class.getSimpleName()
            + "@" + "xml-generation-always-include-optional-elements";

    public final static String PRETTY_PRINT_RESPONSE_MESSAGES = WsdlSettings.class.getSimpleName() + "@"
            + "pretty-print-response-xml";

    public final static String ATTACHMENT_PARTS = WsdlSettings.class.getSimpleName() + "@" + "attachment-parts";

    public final static String ALLOW_INCORRECT_CONTENTTYPE = WsdlSettings.class.getSimpleName() + "@"
            + "allow-incorrect-contenttype";

    public final static String ENABLE_MTOM = WsdlSettings.class.getSimpleName() + "@" + "enable-mtom";

    public static final String SCHEMA_DIRECTORY = WsdlSettings.class.getSimpleName() + "@" + "schema-directory";

    public final static String NAME_WITH_BINDING = WsdlSettings.class.getSimpleName() + "@" + "name-with-binding";

    public final static String EXCLUDED_TYPES = WsdlSettings.class.getSimpleName() + "@" + "excluded-types";

    public final static String STRICT_SCHEMA_TYPES = WsdlSettings.class.getSimpleName() + "@" + "strict-schema-types";

    public final static String COMPRESSION_LIMIT = WsdlSettings.class.getSimpleName() + "@" + "compression-limit";

    public final static String PRETTY_PRINT_PROJECT_FILES = WsdlSettings.class.getSimpleName() + "@"
            + "pretty-print-project-files";

    public static final String XML_GENERATION_SKIP_COMMENTS = WsdlSettings.class.getSimpleName() + "@"
            + "xml-generation-skip-comments";

    public final static String TRIM_WSDL = WsdlSettings.class.getSimpleName() + "@"
            + "trim-wsdl";

}
