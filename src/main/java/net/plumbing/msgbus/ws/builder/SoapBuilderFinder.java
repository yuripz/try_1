package net.plumbing.msgbus.ws.builder;

import net.plumbing.msgbus.ws.SoapContext;

import javax.xml.namespace.QName;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */

public interface SoapBuilderFinder {

    SoapBuilderFinder name(String name);

    SoapBuilderFinder name(QName name);

    SoapBuilderFinder namespaceURI(String namespaceURI);

    SoapBuilderFinder localPart(String localPart);

    SoapBuilderFinder prefix(String prefix);

    SoapBuilder find();

    SoapBuilder find(SoapContext context);

}

