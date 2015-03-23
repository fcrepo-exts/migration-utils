package org.fcrepo.migration.foxml11;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DC {
    
    public static final String DC_NS = "http://purl.org/dc/elements/1.1/";
    
    @XmlElement(name="contributor", namespace=DC_NS)
    public String[] contributor;

    @XmlElement(name="coverage", namespace=DC_NS)
    public String[] coverage;

    @XmlElement(name="creator", namespace=DC_NS)
    public String[] creator;

    @XmlElement(name="date", namespace=DC_NS)
    public String[] date;

    @XmlElement(name="description", namespace=DC_NS)
    public String[] description;

    @XmlElement(name="format", namespace=DC_NS)
    public String[] format;

    @XmlElement(name="identifier", namespace=DC_NS)
    public String[] identifier;
    
    @XmlElement(name="language", namespace=DC_NS)
    public String[] language;

    @XmlElement(name="publisher", namespace=DC_NS)
    public String[] publisher;

    @XmlElement(name="relation", namespace=DC_NS)
    public String[] relation;

    @XmlElement(name="rights", namespace=DC_NS)
    public String[] rights;

    @XmlElement(name="source", namespace=DC_NS)
    public String[] source;
    
    @XmlElement(name="subject", namespace=DC_NS)
    public String[] subject;

    @XmlElement(name="title", namespace=DC_NS)
    public String[] title;

    @XmlElement(name="type", namespace=DC_NS)
    public String[] type;
    
    public List<String> getRepresentedElementURIs() {
        final List<String> result = new ArrayList<String>();
        for (Field f : DC.class.getDeclaredFields()) {
            for (Annotation a : f.getAnnotations()) {
                if (a.annotationType().equals(XmlElement.class)) {
                    final XmlElement e = (XmlElement) a;
                    try {
                        if (f.get(this) != null) {
                            result.add(e.namespace() + e.name());
                            break;
                        }
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return result;
    }
    
    public List<String> getValuesForURI(final String uri) {
        try {
            final String fieldName = uri.substring(uri.lastIndexOf('/') + 1);
            final Field field = DC.class.getField(fieldName);
            String[] values = (String[]) field.get(this);
            if (values != null) {
                return Arrays.asList(values);
            } else {
                return null;
            }
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(uri + " not recognized as a DC element!");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    public static DC parseDC(InputStream is) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(DC.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        JAXBElement<DC> p = unmarshaller.unmarshal(new StreamSource(is), DC.class);
        return p.getValue();
    }
    
    
}
