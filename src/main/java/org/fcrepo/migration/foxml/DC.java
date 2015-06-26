package org.fcrepo.migration.foxml;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.transform.stream.StreamSource;
/**
 *
 * @author mdurbin
 *
 */
public class DC {

    public static final String DC_NS = "http://purl.org/dc/elements/1.1/";

    @XmlElement(name = "contributor", namespace = DC_NS)
    public String[] contributor;

    @XmlElement(name = "coverage", namespace = DC_NS)
    public String[] coverage;

    @XmlElement(name = "creator", namespace = DC_NS)
    public String[] creator;

    @XmlElement(name = "date", namespace = DC_NS)
    public String[] date;

    @XmlElement(name = "description", namespace = DC_NS)
    public String[] description;

    @XmlElement(name = "format", namespace = DC_NS)
    public String[] format;

    @XmlElement(name = "identifier", namespace = DC_NS)
    public String[] identifier;

    @XmlElement(name = "language", namespace = DC_NS)
    public String[] language;

    @XmlElement(name = "publisher", namespace = DC_NS)
    public String[] publisher;

    @XmlElement(name = "relation", namespace = DC_NS)
    public String[] relation;

    @XmlElement(name = "rights", namespace = DC_NS)
    public String[] rights;

    @XmlElement(name = "source", namespace = DC_NS)
    public String[] source;

    @XmlElement(name = "subject", namespace = DC_NS)
    public String[] subject;

    @XmlElement(name = "title", namespace = DC_NS)
    public String[] title;

    @XmlElement(name = "type", namespace = DC_NS)
    public String[] type;

    /**
     * get represented element uris
     * @return the list
     */
    public List<String> getRepresentedElementURIs() {
        final List<String> result = new ArrayList<String>();
        for (final Field f : DC.class.getDeclaredFields()) {
            for (final Annotation a : f.getAnnotations()) {
                if (a.annotationType().equals(XmlElement.class)) {
                    final XmlElement e = (XmlElement) a;
                    try {
                        if (f.get(this) != null) {
                            result.add(e.namespace() + e.name());
                            break;
                        }
                    } catch (final IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return result;
    }

    /**
     * get values for uri
     * @param uri the uri
     * @return the value list
     */
    public List<String> getValuesForURI(final String uri) {
        try {
            final String fieldName = uri.substring(uri.lastIndexOf('/') + 1);
            final Field field = DC.class.getField(fieldName);
            final String[] values = (String[]) field.get(this);
            if (values != null) {
                return Arrays.asList(values);
            } else {
                return null;
            }
        } catch (final NoSuchFieldException e) {
            throw new RuntimeException(uri + " not recognized as a DC element!");
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * parse DC
     * @param is the input stream
     * @return the DC
     * @throws JAXBException JAXB exception
     */
    public static DC parseDC(final InputStream is) throws JAXBException {
        final JAXBContext jc = JAXBContext.newInstance(DC.class);
        final Unmarshaller unmarshaller = jc.createUnmarshaller();
        final JAXBElement<DC> p = unmarshaller.unmarshal(new StreamSource(is), DC.class);
        return p.getValue();
    }


}
