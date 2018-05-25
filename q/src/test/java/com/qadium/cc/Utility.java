package com.qadium.cc;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Utility {

    public static JsonObject getJsonObject(String json) {
        JsonElement element = new Gson().fromJson (json, JsonElement.class);
        return element.getAsJsonObject();
    }


}
