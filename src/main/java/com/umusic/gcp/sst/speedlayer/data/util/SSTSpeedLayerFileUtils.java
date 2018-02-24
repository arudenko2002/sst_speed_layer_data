package com.umusic.gcp.sst.speedlayer.data.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

/**
 * Created by arumugv on 2/22/17.
 * File Utility class
 */
public class SSTSpeedLayerFileUtils {

    /**
     * loads file from classpath and returns the content as string
     * @param resourceName
     * @return
     * @throws IOException
     */
    public static String loadFilefromClasspath(final String resourceName) throws IOException {
        String content = null;
        try (final InputStream ipStream =
                     SSTSpeedLayerFileUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
            content = IOUtils.toString(ipStream);
        }

        return content;
    }
}
