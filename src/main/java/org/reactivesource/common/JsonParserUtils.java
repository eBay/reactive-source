/*******************************************************************************
 * Copyright (c) 2013-2014 eBay Software Foundation
 *
 * See the file license.txt for copying permission.
 ******************************************************************************/

package org.reactivesource.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.util.Assert.notNull;

@SuppressWarnings("unchecked")
public class JsonParserUtils {

    public static Map<String, Object> jsonStringToMap(String jsonString) {
        notNull(jsonString, "jsonString can not be null");
        return jsonObjectToMap(new JSONObject(jsonString));
    }

    public static Map<String, Object> jsonObjectToMap(JSONObject jsonObject) {
        notNull(jsonObject, "jsonString can not be null");
        try {
            return new ObjectMapper().readValue(jsonObject.toString(), HashMap.class);
        } catch (IOException e) {
            throw new JSONException("Could not map row entity:" + jsonObject.toString());
        }
    }


}
