package net.plumbing.msgbus.ws.builder;

import net.plumbing.msgbus.ws.SoapContext;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
public interface SoapOperationFinder {

    SoapOperationFinder name(String operationName);

    SoapOperationFinder soapAction(String soapAction);

    SoapOperationFinder inputName(String inputName);

    SoapOperationFinder outputName(String inputName);

    SoapOperationBuilder find();

    SoapOperationBuilder find(SoapContext context);


}
