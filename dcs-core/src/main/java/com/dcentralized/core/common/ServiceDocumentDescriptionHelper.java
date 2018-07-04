/*
 * Copyright (c) 2017 dCentralizedSystems, LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.dcentralized.core.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper methods for constructing ServiceDocumentDescriptions
 *
 * Package private - Infrastructure use only
 */
class ServiceDocumentDescriptionHelper {

    private static final Logger logger = Logger.getLogger(ServiceDocumentDescriptionHelper.class.getName());

    /** WeakHashMap &gt;Service/Doc class, Map&gt;Description Key,Description Text&lt;&lt; */
    private static final Map<Class<?>, Map<String,String>> documentationDescriptionCache =
            new WeakHashMap<>();

    /**
     * Lookup table for whether or not a given action should
     * have a default request type (if true) or both request and response type (false) or neither (null)
     */
    private static final Map<String, Boolean> actionDefaultTypeMap;

    static {
        actionDefaultTypeMap = new LinkedHashMap<>();
        actionDefaultTypeMap.put("Get", false);
        actionDefaultTypeMap.put("Post", true);
        actionDefaultTypeMap.put("Put", true);
        actionDefaultTypeMap.put("Patch", true);
        actionDefaultTypeMap.put("Delete", null);
    }

    private ServiceDocumentDescriptionHelper() {
        // do nothing
    }

    /**
     * The description field can optionally be used as a key into an HTML document containing more complete documentation
     * so as to avoid including massive documentation inside the Java sources, and to permit tech-pubs
     * authors to edit an HTML file instead of modifying in-line descriptions inside Java files.
     * <p>
     * If there is no resource file, or if the file does not contain the description, the description is used as-is.
     */
    public static String lookupDocumentationDescription(Class<?> clazz, String description) {

        if (description == null) {
            return null;
        }

        if (!documentationDescriptionCache.containsKey(clazz)) {
            // this document type has not yet been cached
            String resourceName = "/" + clazz.getName().replaceAll("\\.", "/") + ".html";
            InputStream is = clazz.getResourceAsStream(resourceName);
            if (is == null) {
                documentationDescriptionCache.put(clazz, null);
                return description;
            }
            Map<String,String> cache = new HashMap<>();
            // very simple parser - each new description mapping starts on a new line with '<h1>' and the description key,
            // which is the contents between an '<h1>' and an '</h1>' termination.
            // The description body must follow on subsequent lines (anything on the same line as the key is ignored).

            String key = null;
            StringBuilder body = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                int lineNo = 1;

                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("<h1>")) {
                        // look for end
                        int index = line.indexOf("</h1>");
                        if (index < 0) {
                            logger.log(Level.WARNING,
                                    "Unexpected format in document description file: %s at line %d",
                                    new Object[]{resourceName, lineNo});
                        } else {
                            if (key != null) {
                                cache.put(key, body.toString().trim());
                            }
                            key = line.substring(4, index).trim();
                            body = new StringBuilder();
                        }
                    } else {
                        body.append(line).append(" ");
                    }
                    lineNo++;
                }
            } catch (IOException ex) {
                Logger.getLogger(ServiceHost.class.getName()).log(Level.SEVERE, null, ex);
            }
            // and add last key/value pair if there is one
            if (body.length() > 0) {
                cache.put(key, body.toString());
            }

            // now store this in cache
            documentationDescriptionCache.put(clazz, cache);
        }

        Map<String,String> cache = documentationDescriptionCache.get(clazz);
        if (cache == null) {
            // no description file, return as-is
            return description;
        }
        String mappedDesc = cache.get(description);
        if (mappedDesc == null) {
            // no mapping for this description, return previous description
            return description;
        }
        return mappedDesc;
    }
}
