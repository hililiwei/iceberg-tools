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

import com.google.common.collect.Sets;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.NotSupportedException;

public class IcebergDatumWriter<D> extends GenericDatumWriter<D> {

    private Set<String> boundKeys = Sets.newHashSet("lower_bounds", "upper_bounds");

    private Boolean isBoundKey = false;

    private Map<Integer, String> icebergFields;

    private int boundKey = 0;

    public IcebergDatumWriter() {
        super(IcebergData.get());
    }

    protected IcebergDatumWriter(IcebergData data) {
        super(data);
    }

    public IcebergDatumWriter(Schema root, Map<Integer, String> icebergFields) {
        this();
        setSchema(root);
        this.icebergFields = icebergFields;
    }

    public IcebergDatumWriter(Schema root, IcebergData data) {
        super(root, data);
    }

    /** Called to write data. */
    protected void write(Schema schema, Object datum, Encoder out) throws IOException {
        LogicalType logicalType = schema.getLogicalType();
        if (datum != null && logicalType != null) {
            Conversion<?> conversion = getData().getConversionByClass(datum.getClass(), logicalType);
            writeWithoutConversion(schema, convert(schema, logicalType, conversion, datum), out);
        } else {
            writeWithoutConversion(schema, datum, out);
        }
    }

    /**
     * Called to write a record. May be overridden for alternate record
     * representations.
     */
    protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
        for (Schema.Field f : schema.getFields()) {
            if (schema.getName().equalsIgnoreCase("r2")){
                if ( boundKeys.contains(f.name().toLowerCase(Locale.ROOT))) {
                    isBoundKey = true;
                } else {
                    isBoundKey = false;
                }
            }
            writeField(datum, f, out, null);
        }
    }

    /**
     * Called to write a single field of a record. May be overridden for more
     * efficient or alternate implementations.
     */
    protected void writeField(Object datum, Schema.Field f, Encoder out, Object state) throws IOException {
        Object value = getData().getField(datum, f.name(), f.pos());
        try {
            write(f.schema(), value, out);
        } catch (final UnresolvedUnionException uue) { // recreate it with the right field info
            final UnresolvedUnionException unresolvedUnionException = new UnresolvedUnionException(f.schema(), f,
                value);
            unresolvedUnionException.addSuppressed(uue);
            throw unresolvedUnionException;
        } catch (NullPointerException e) {
            throw npe(e, " in field " + f.name());
        } catch (ClassCastException cce) {
            throw addClassCastMsg(cce, " in field " + f.name());
        } catch (AvroTypeException ate) {
            throw addAvroTypeMsg(ate, " in field " + f.name());
        }
    }

    /** Called to write data. */
    protected void writeWithoutConversion(Schema schema, Object datum, Encoder out) throws IOException {
        try {
            switch (schema.getType()) {
                case RECORD:
                    writeRecord(schema, datum, out);
                    break;
                case ENUM:
                    writeEnum(schema, datum, out);
                    break;
                case ARRAY:
                    writeArray(schema, datum, out);
                    break;
                case MAP:
                    writeMap(schema, datum, out);
                    break;
                case UNION:
                    int index = resolveUnion(schema, datum);
                    out.writeIndex(index);
                    write(schema.getTypes().get(index), datum, out);
                    break;
                case FIXED:
                    writeFixed(schema, datum, out);
                    break;
                case STRING:
                    writeString(schema, datum, out);
                    break;
                case BYTES:
                    if (isBoundKey) {
                        ByteBuffer byteBuffer = bound2Byte(datum, out);
                        writeBytes(byteBuffer, out);
                        break;
                    } else {
                        writeBytes(datum, out);
                        break;
                    }
                case INT:
                    if (isBoundKey) {
                        boundKey = (Integer) datum;
                    }
                    out.writeInt(((Number) datum).intValue());
                    break;
                case LONG:
                    out.writeLong(((Number) datum).longValue());
                    break;
                case FLOAT:
                    out.writeFloat(((Number) datum).floatValue());
                    break;
                case DOUBLE:
                    out.writeDouble(((Number) datum).doubleValue());
                    break;
                case BOOLEAN:
                    out.writeBoolean((Boolean) datum);
                    break;
                case NULL:
                    out.writeNull();
                    break;
                default:
                    error(schema, datum);
            }
        } catch (NullPointerException e) {
            throw npe(e, " of " + schema.getFullName());
        }
    }

    // /** Called to write data. */
    // protected void writeWithoutConversion(Object datum, Encoder out) throws IOException {
    //     try {
    //         if (Integer.class.equals(datum.getClass())) {
    //             out.writeInt((Integer) datum);
    //         } else if (String.class.equals(datum.getClass()) || UUID.class.equals(datum.getClass())) {
    //             out.writeString((CharSequence) datum);
    //         } else if (Long.class.equals(datum.getClass())) {
    //             out.writeLong((Long) datum);
    //         } else if (Float.class.equals(datum.getClass())) {
    //             out.writeFloat((Float) datum);
    //         } else if (Double.class.equals(datum.getClass())) {
    //             out.writeDouble((Double) datum);
    //         } else {
    //             error(datum);
    //         }
    //     } catch (NullPointerException e) {
    //         throw npe(e, "");
    //     }
    // }

    private void error(Object datum) {
        throw new NotSupportedException("Not Supported" + ": " + datum);
    }

    private void error(Schema schema, Object datum) {
        throw new NotSupportedException("Not a " + schema + ": " + datum);
    }

    protected ByteBuffer bound2Byte(Object datum, Encoder out) throws IOException {
        String types = icebergFields.get(boundKey).toLowerCase(Locale.ROOT);
        Type.PrimitiveType primitiveType = Types.fromPrimitiveString(types);
        Object metricValue = Conversions.fromByteBuffer(primitiveType, (ByteBuffer) datum);
        String valueStr = metricValue.toString();
        String type = primitiveType.toString();
        String result = "value:" + valueStr + ";type:" + type;
       return ByteBuffer.wrap(result.getBytes(StandardCharsets.UTF_8));
    }
}
