package com.umusic.gcp.sst.speedlayer.data.util;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

/**
 * Created by arumugv on 2/22/17.
 * Avro utility class
 */
public class AvroSchemaUtil {

    /**
     * Loads Avro Schema from avsc file in classpath
     *
     * @param resourceName avsc resource for the schema.
     * @return Avro Schema.
     * @throws IOException on File I/O.
     */
    public static Schema getSchemaFromClasspath(final String resourceName) throws IOException {
        Schema schema = null;
        try (final InputStream ipStream =
                     AvroSchemaUtil.class.getClassLoader().getResourceAsStream(resourceName)) {
            schema = getSchemaFromString(IOUtils.toString(ipStream));
        }

        return schema;
    }

    /**
     * Loads Avro Schema from avsc string.
     *
     * @param schemaStr Avsc String
     * @return Avro Schema
     * @throws IOException
     */
    public static Schema getSchemaFromString(final String schemaStr) throws IOException {
        return new Schema.Parser().parse(schemaStr);
    }


}