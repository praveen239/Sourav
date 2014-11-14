/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.reltio.QuickTools;

/**
 *
 * @author admin
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class Decompressor {
    public static final byte[] COMPRESS_DETECTOR = "\u262D".getBytes(StandardCharsets.UTF_8);

    private static String unzipValue(byte[] value) {
        String unzippedValue = null;
        try {
            try (
                    InputStreamReader reader = new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(value)), StandardCharsets.UTF_8);
                    StringWriter writer = new StringWriter()
            ) {
                char[] buffer = new char[10240];
                for (int length = 0; (length = reader.read(buffer)) > 0; ) {
                    writer.write(buffer, 0, length);
                }

                unzippedValue = writer.toString();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return unzippedValue;
    }

   private static boolean isCompressedValue(byte[] valueArr) {
        boolean isCompressed = true;
        if (valueArr.length <= COMPRESS_DETECTOR.length) {
            isCompressed = false;
        } else {
            for (int i = 0; i < COMPRESS_DETECTOR.length; i++) {
                if (valueArr[i] != COMPRESS_DETECTOR[i]) {
                    isCompressed = false;
                }
            }
        }

        return isCompressed;
    }

    public static String decompress(byte[] value) {
        if (isCompressedValue(value)) {
            return unzipValue(Arrays.copyOfRange(value, COMPRESS_DETECTOR.length, value.length));
        } else {
            //return new String(value, StandardCharsets.UTF_8);
        	return null;
        }

    }
}