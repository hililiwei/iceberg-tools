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
 *
 * The section demarcated by 'copied from Apache commons-codec' is
 * from Apache Commons Codec v1.9.
 */

package dev.liliwei.iceberg.tool;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/** Static utility methods for tools. */
public class Util {
    /**
     * Returns stdin if filename is "-", else opens the File in the owning
     * filesystem and returns an InputStream for it. Relative paths will be opened
     * in the default filesystem.
     *
     * @param filename The filename to be opened
     * @throws IOException
     */
    public static BufferedInputStream fileOrStdin(String filename, InputStream stdin) throws IOException {
        return new BufferedInputStream(filename.equals("-") ? stdin : openFromFS(filename));
    }

    /**
     * Returns an InputStream for the file using the owning filesystem, or the
     * default if none is given.
     *
     * @param filename The filename to be opened
     * @throws IOException
     */
    static InputStream openFromFS(String filename) throws IOException {
        Path p = new Path(filename);
        return p.getFileSystem(new Configuration()).open(p);
    }

    /**
     * Closes the inputstream created from {@link Util.fileOrStdin} unless it is
     * System.in.
     *
     * @param in The inputstream to be closed.
     */
    static void close(InputStream in) {
        if (!System.in.equals(in)) {
            try {
                in.close();
            } catch (IOException e) {
                System.err.println("could not close InputStream " + in.toString());
            }
        }
    }

    /**
     * Parses a schema from the specified file.
     *
     * @param filename The file name to parse
     * @return The parsed schema
     * @throws IOException
     */
    public static Schema parseSchemaFromFS(String filename) throws IOException {
        InputStream stream = openFromFS(filename);
        try {
            return new Schema.Parser().parse(stream);
        } finally {
            close(stream);
        }
    }
}
