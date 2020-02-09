/*
 * Copyright (c) 2018-2019 dCentralizedSystems, LLC. All Rights Reserved.
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

package com.dcentralized.core.common.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import com.dcentralized.core.common.ServiceStats;
import com.dcentralized.core.common.ServiceStats.ServiceStat;
import com.dcentralized.core.common.ServiceStats.TimeSeriesStats;
import com.dcentralized.core.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.dcentralized.core.common.ServiceStats.TimeSeriesStats.ExtendedTimeBin;
import com.dcentralized.core.common.ServiceStats.TimeSeriesStats.TimeBin;
import com.dcentralized.core.common.Utils;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

/**
 * GSON {@link JsonSerializer}/{@link JsonDeserializer} for representing {@link ServiceStat}s of objects
 * keyed by strings, whereby the objects are themselves serialized as JSON objects.
 */
public enum ServiceStatTypeConverter
        implements JsonSerializer<ServiceStat>, JsonDeserializer<ServiceStat> {
    INSTANCE;

    public static final Type TYPE = new TypeToken<ServiceStat>() {}.getType();

    private double round(double v, Integer roundingFactor) {
        if (roundingFactor == null) {
            return v;
        }
        v = v * roundingFactor;
        v = Math.round(v);
        v = v / roundingFactor;
        return v;
    }

    @Override
    public JsonElement serialize(ServiceStat stat,
            Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject jo = new JsonObject();
        jo.addProperty("name", stat.name);
        if (stat.unit != null) {
            jo.addProperty("unit", stat.unit);
        }
        jo.addProperty("accumulatedValue", stat.accumulatedValue);
        jo.addProperty("lastUpdateMicrosUtc", stat.lastUpdateMicrosUtc);
        jo.addProperty("latestValue", stat.latestValue);
        jo.addProperty("version", stat.version);
        if (stat.sourceTimeMicrosUtc != null) {
            jo.addProperty("sourceTimeMicrosUtc", stat.sourceTimeMicrosUtc);
        }
        if (stat.serviceReference != null) {
            jo.addProperty("serviceReference", stat.serviceReference.toString());
        }
        if (stat.logHistogram != null) {
            jo.add("logHistogram", context.serialize(stat.logHistogram));
        }

        if (stat.timeSeriesStats == null || stat.timeSeriesStats.bins == null
                || stat.timeSeriesStats.bins.isEmpty()) {
            return jo;
        }
        synchronized (stat) {
            JsonObject jsonTimeseries = new JsonObject();
            jsonTimeseries.addProperty("binDurationMillis", stat.timeSeriesStats.binDurationMillis);
            jsonTimeseries.addProperty("numBins", stat.timeSeriesStats.numBins);

            int aggBitMask = 0;
            for (AggregationType agt : stat.timeSeriesStats.aggregationType) {
                aggBitMask |= 1 << agt.ordinal();
            }

            jsonTimeseries.addProperty("agg", aggBitMask);
            if (stat.timeSeriesStats.roundingFactor != TimeSeriesStats.DEFAULT_ROUNDING_FACTOR) {
                jsonTimeseries.addProperty("roundingFactor", stat.timeSeriesStats.roundingFactor);
            }
            JsonObject jsonBins = new JsonObject();

            long baseId = 0;
            Double maxValue = Double.NEGATIVE_INFINITY;
            Double minValue = Double.POSITIVE_INFINITY;
            for (Entry<Long, TimeBin> e : stat.timeSeriesStats.bins.entrySet()) {
                long binId = e.getKey();
                binId /= stat.timeSeriesStats.binDurationMillis;
                if (baseId == 0) {
                    baseId = binId;
                }
                binId -= baseId;

                JsonObject bin = new JsonObject();
                TimeBin tb = e.getValue();
                if (tb.count == 0) {
                    if (tb instanceof ExtendedTimeBin) {
                        ExtendedTimeBin etb = (ExtendedTimeBin) tb;
                        if (etb.sum == null) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                double v = 0;
                if (tb.count > 0) {
                    v = round(tb.avg, stat.timeSeriesStats.roundingFactor);
                    bin.addProperty("a", v);
                    if (v < minValue) {
                        minValue = v;
                    }
                    if (v > maxValue) {
                        maxValue = v;
                    }
                    v = round(tb.var, stat.timeSeriesStats.roundingFactor);
                    if (v > 0) {
                        bin.addProperty("v", v);
                    }
                }

                if (tb instanceof ExtendedTimeBin) {
                    ExtendedTimeBin etb = (ExtendedTimeBin) tb;
                    if (etb.sum != null) {
                        v = round(etb.sum, stat.timeSeriesStats.roundingFactor);
                        bin.addProperty("s", v);
                    }
                    if (etb.max != null) {
                        v = round(etb.max, stat.timeSeriesStats.roundingFactor);
                        bin.addProperty("mx", v);
                        if (v < minValue) {
                            minValue = v;
                        }
                        if (v > maxValue) {
                            maxValue = v;
                        }
                    }
                    if (etb.min != null) {
                        v = round(etb.min, stat.timeSeriesStats.roundingFactor);
                        bin.addProperty("mn", v);
                        if (v < minValue) {
                            minValue = v;
                        }
                        if (v > maxValue) {
                            maxValue = v;
                        }
                    }
                    if (etb.latest != null) {
                        v = round(etb.latest, stat.timeSeriesStats.roundingFactor);
                        bin.addProperty("lt", v);
                    }
                }
                if (tb.count > 1) {
                    bin.addProperty("c", tb.count);
                }
                jsonBins.add("" + binId, bin);
            }
            jsonTimeseries.add("bins", jsonBins);
            jsonTimeseries.addProperty("baseBinTimeMillis", baseId);
            jsonTimeseries.addProperty("min", minValue);
            jsonTimeseries.addProperty("max", maxValue);
            jo.add("timeSeriesStats", jsonTimeseries);
        }
        return jo;
    }

    @Override
    public ServiceStat deserialize(JsonElement json, Type unused,
            JsonDeserializationContext context)
            throws JsonParseException {

        ServiceStat result = new ServiceStat();
        if (!json.isJsonObject()) {
            throw new JsonParseException("Expecting a json object but found: " + json);
        }
        JsonObject jsonObject = json.getAsJsonObject();
        result.name = jsonObject.get("name").getAsString();
        result.accumulatedValue = jsonObject.get("accumulatedValue").getAsDouble();
        result.lastUpdateMicrosUtc = jsonObject.get("lastUpdateMicrosUtc").getAsLong();
        result.latestValue = jsonObject.get("latestValue").getAsLong();
        result.version = jsonObject.get("version").getAsLong();
        JsonElement e = jsonObject.get("unit");
        if (e != null) {
            result.unit = e.getAsString();
        }
        e = jsonObject.get("serviceReference");
        if (e != null) {
            try {
                result.serviceReference = new URI(e.getAsString());
            } catch (Throwable ex) {
                Utils.logWarning("%s", ex.toString());
            }
        }
        e = jsonObject.get("sourceTimeMicrosUtc");
        if (e != null) {
            result.sourceTimeMicrosUtc = e.getAsLong();
        }
        e = jsonObject.get("logHistogram");
        if (e != null) {
            result.logHistogram = context.deserialize(e,
                    ServiceStats.ServiceStatLogHistogram.class);
        }
        e = jsonObject.get("timeSeriesStats");
        if (e == null) {
            return result;
        }

        JsonObject timeSeriesStats = e.getAsJsonObject();
        result.timeSeriesStats = new ServiceStats.TimeSeriesStats();
        result.timeSeriesStats.numBins = timeSeriesStats.get("numBins").getAsInt();
        result.timeSeriesStats.binDurationMillis = timeSeriesStats.get("binDurationMillis")
                .getAsInt();

        JsonElement baseBinElem = timeSeriesStats.get("baseBinTimeMillis");
        Long baseBinTimeMillis = null;
        if (baseBinElem != null) {
            baseBinTimeMillis = baseBinElem.getAsLong();
        }
        e = timeSeriesStats.get("roundingFactor");
        if (e != null) {
            result.timeSeriesStats.roundingFactor = e.getAsInt();
        }

        EnumSet<AggregationType> templateAgg = TimeSeriesStats.AGG_ALL_INSTANCE;
        result.timeSeriesStats.aggregationType = EnumSet.noneOf(AggregationType.class);
        int aggBitMask = timeSeriesStats.get("agg").getAsInt();
        for (AggregationType ag : templateAgg) {
            if ((aggBitMask & (1 << ag.ordinal())) > 0) {
                result.timeSeriesStats.aggregationType.add(ag);
            }
        }

        result.timeSeriesStats.bins = new ConcurrentSkipListMap<>();
        for (Entry<String, JsonElement> en : timeSeriesStats.get("bins").getAsJsonObject()
                .entrySet()) {
            long binId = Long.parseLong(en.getKey());
            if (baseBinTimeMillis == null) {
                baseBinTimeMillis = binId;
            } else {
                binId += baseBinTimeMillis;
            }

            long binTimeMillis = binId * result.timeSeriesStats.binDurationMillis;
            TimeBin bin = null;
            ExtendedTimeBin ebin = null;
            if (result.timeSeriesStats.aggregationType.contains(AggregationType.SUM)
                    || result.timeSeriesStats.aggregationType.contains(AggregationType.LATEST)
                    || result.timeSeriesStats.aggregationType.contains(AggregationType.MAX)
                    || result.timeSeriesStats.aggregationType.contains(AggregationType.MIN)) {
                ebin = new ExtendedTimeBin();
                bin = ebin;
            } else {
                bin = new TimeBin();
            }
            JsonObject sBin = en.getValue().getAsJsonObject();
            e = sBin.get("a");
            if (e != null) {
                bin.avg = e.getAsDouble();
                bin.var = 0.0;
            }
            e = sBin.get("s");
            if (e != null) {
                ebin.sum = e.getAsDouble();
            }
            e = sBin.get("v");
            if (e != null) {
                bin.var = e.getAsDouble();
            }
            e = sBin.get("lt");
            if (e != null) {
                ebin.latest = e.getAsDouble();
            }
            e = sBin.get("mn");
            if (e != null) {
                ebin.min = e.getAsDouble();
            }
            e = sBin.get("mx");
            if (e != null) {
                ebin.max = e.getAsDouble();
            }
            e = sBin.get("c");
            if (e != null) {
                bin.count = e.getAsDouble();
            } else {
                bin.count = 1;
            }

            result.timeSeriesStats.bins.put(binTimeMillis, bin);
        }

        return result;
    }

}
