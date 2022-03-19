/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.liliwei.iceberg.tool;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/** Reads a data file and dumps to JSON */
public class ManifestFileReadTool implements Tool {
    private static final long DEFAULT_HEAD_COUNT = 10;

    private static long getHeadCount(OptionSet optionSet, OptionSpec<String> headOption, List<String> nargs) {
        long headCount = Long.MAX_VALUE;
        if (optionSet.has(headOption)) {
            headCount = DEFAULT_HEAD_COUNT;
            List<String> headValues = optionSet.valuesOf(headOption);
            if (headValues.size() > 0) {
                // if the value parses to int, assume it's meant to go with --head
                // otherwise assume it was an optionSet.nonOptionArgument and add back to the
                // list
                // TODO: support input filenames whose whole path+name is int parsable?
                try {
                    headCount = Long.parseLong(headValues.get(0));
                    if (headCount < 0) {
                        throw new AvroRuntimeException("--head count must not be negative");
                    }
                } catch (NumberFormatException ex) {
                    nargs.addAll(headValues);
                }
            }
        }
        return headCount;
    }

    @Override
    public String getName() {
        return "manifest2json";
    }

    @Override
    public String getShortDescription() {
        return "Dumps an Iceberg Manifest Avro data file as JSON, record per line or pretty.";
    }

    @Override
    public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
        OptionParser optionParser = new OptionParser();
        OptionSpec<Void> prettyOption = optionParser.accepts("pretty", "Turns on pretty printing.");
        String headDesc = String.format("Converts the first X records (default is %d).", DEFAULT_HEAD_COUNT);
        OptionSpec<String> headOption = optionParser.accepts("head", headDesc).withOptionalArg();
        OptionSpec<String> readerSchemaFileOption = optionParser.accepts("reader-schema-file", "Reader schema file")
            .withOptionalArg()
            .ofType(String.class);
        OptionSpec<String> readerSchemaOption = optionParser.accepts("reader-schema", "Reader schema")
            .withOptionalArg()
            .ofType(String.class);

        OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
        Boolean pretty = optionSet.has(prettyOption);
        List<String> nargs = new ArrayList<>((List<String>) optionSet.nonOptionArguments());

        String readerSchemaStr = readerSchemaOption.value(optionSet);
        String readerSchemaFile = readerSchemaFileOption.value(optionSet);

        Schema readerSchema = null;
        if (readerSchemaFile != null) {
            readerSchema = Util.parseSchemaFromFS(readerSchemaFile);
        } else if (readerSchemaStr != null) {
            readerSchema = new Schema.Parser().parse(readerSchemaStr);
        }

        long headCount = getHeadCount(optionSet, headOption, nargs);

        if (nargs.size() != 2) {
            printHelp(err);
            err.println();
            optionParser.printHelpOn(err);
            return 1;
        }

        BufferedInputStream inStream = Util.fileOrStdin(nargs.get(0), stdin);

        JsonReader jsonReader = new Gson().newJsonReader(new BufferedReader(new FileReader(nargs.get(1))));
        Map<Integer, String> icebergFields = parseMetaData(jsonReader);

        GenericDatumReader<Object> reader = new GenericDatumReader<>();
        if (readerSchema != null) {
            reader.setExpected(readerSchema);
        }
        try (DataFileStream<Object> streamReader = new DataFileStream<>(inStream, reader)) {
            Schema schema = readerSchema != null ? readerSchema : streamReader.getSchema();
            DatumWriter writer = new IcebergDatumWriter<>(schema, icebergFields);
            IcebergEncoder encoder = new IcebergEncoder(schema, out, pretty);
            out.println("[");
            for (long recordCount = 0; streamReader.hasNext() && recordCount < headCount; recordCount++) {
                Object datum = streamReader.next();
                writer.write(datum, encoder);
                encoder.flush();
                if (streamReader.hasNext()) {
                    out.print(",");
                }
            }
            encoder.flush();
            out.println();
            out.println("]");
            out.flush();
        }
        return 0;
    }

    private void printHelp(PrintStream ps) {
        ps.println("tojson [--pretty] [--head[=X]] manifest-file metadata-json-file");
        ps.println();
        ps.println(getShortDescription());
        ps.println("A dash ('-') can be given as an input file to use stdin");
    }

    public Map<Integer, String> parseMetaData(JsonReader jsonReader) {
        Map<Integer, String> fieldMap = Maps.newHashMap();

        JsonElement root = JsonParser.parseReader(jsonReader);
        JsonObject asJsonObject = root.getAsJsonObject();

        AtomicReference<JsonObject> schemaRef = new AtomicReference<>(asJsonObject.getAsJsonObject("schema"));
        if (schemaRef.get() == null) {
            int schemaId = asJsonObject.get("current-schema-id").getAsInt();
            JsonArray schemas = asJsonObject.getAsJsonArray("schemas");
            schemas.forEach(schemaArr -> {
                JsonObject schemaObj = schemaArr.getAsJsonObject();
                if (schemaObj.get("schema-id").getAsInt() == schemaId) {
                    schemaRef.set(schemaObj);
                }
            });
        }
        JsonArray fields = schemaRef.get().getAsJsonArray("fields");
        Iterator<JsonElement> iterator = fields.iterator();
        iterator.forEachRemaining(fieldJson -> {
            JsonObject field = fieldJson.getAsJsonObject();
            fieldMap.put(field.get("id").getAsInt(), field.get("type").getAsString());
        });
        return fieldMap;
    }
}
