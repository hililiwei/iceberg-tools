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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("deprecation")
public class TestDataFileTools {
    static final int COUNT = 15;

    private static final String KEY_NEEDING_ESCAPES = "trn\\\r\t\n";

    private static final String ESCAPED_KEY = "trn\\\\\\r\\t\\n";

    @ClassRule public static TemporaryFolder DIR = new TemporaryFolder();

    static File sampleFile;

    static String jsonData;

    static Schema schema;

    static File schemaFile;

    @BeforeClass
    public static void writeSampleFile() throws IOException {
        sampleFile = new File(DIR.getRoot(), TestDataFileTools.class.getName() + ".avro");
        schema = Schema.create(Type.INT);
        schemaFile = new File(DIR.getRoot(), "schema-temp.schema");
        try (FileWriter fw = new FileWriter(schemaFile)) {
            fw.append(schema.toString());
        }

        StringBuilder builder = new StringBuilder();
        try (DataFileWriter<Object> writer =
                new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.setMeta(KEY_NEEDING_ESCAPES, "");
            writer.create(schema, sampleFile);

            for (int i = 0; i < COUNT; ++i) {
                builder.append(Integer.toString(i));
                builder.append("\n");
                writer.append(i);
            }
        }

        jsonData = builder.toString();
    }

    private String run(Tool tool, String... args) throws Exception {
        return run(tool, null, args);
    }

    private String run(Tool tool, InputStream stdin, String... args) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream p = new PrintStream(baos);
        tool.run(
                stdin,
                p, // stdout
                null, // stderr
                Arrays.asList(args));
        return baos.toString("UTF-8").replace("\r", "");
    }

    @Test
    public void testReadToJsonPretty2() throws Exception {
        ManifestFileReadTool dataFileReadTool = new ManifestFileReadTool();

        Path resourceDirectory = Paths.get("src", "test", "resources");

        String[] args =
                new String[] {
                    "tojson",
                    resourceDirectory.toString()
                            + File.separatorChar
                            + "1702989a-f66f-423a-aaf1-a01b9a699685-m0.avro",
                    resourceDirectory.toString() + File.separatorChar + "v1.metadata.json"
                };

        dataFileReadTool.run(
                System.in, System.out, System.err, Arrays.asList(args).subList(1, args.length));
    }

    @Test
    public void testReadMetaData() throws Exception {
        ManifestFileReadTool dataFileReadTool = new ManifestFileReadTool();
        JsonReader jsonReader =
                new Gson()
                        .newJsonReader(
                                new BufferedReader(
                                        new FileReader(
                                                this.getClass()
                                                        .getResource("/v1.metadata.json")
                                                        .getFile())));
        Map<Integer, String> integerStringMap = dataFileReadTool.parseMetaData(jsonReader);
        Assert.assertTrue(integerStringMap.containsKey(1));
        Assert.assertEquals(integerStringMap.get(1), "long");

        Assert.assertTrue(integerStringMap.containsKey(2));
        Assert.assertEquals(integerStringMap.get(2), "string");
    }
}
