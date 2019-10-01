/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UserTypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * A user defined type.
 *
 * A user type is really just a tuple type on steroids.
 */
public class UserType extends TupleType
{
    public final String keyspace;
    public final ByteBuffer name;
    private final List<ByteBuffer> fieldNames;
    private final List<String> stringFieldNames;
    private final UserTypeSerializer serializer;

    public UserType(String keyspace, ByteBuffer name, List<ByteBuffer> fieldNames, List<AbstractType<?>> fieldTypes)
    {
        super(fieldTypes);
        assert fieldNames.size() == fieldTypes.size();
        this.keyspace = keyspace;
        this.name = name;
        this.fieldNames = fieldNames;
        this.stringFieldNames = new ArrayList<>(fieldNames.size());
        LinkedHashMap<String , TypeSerializer<?>> fieldSerializers = new LinkedHashMap<>(fieldTypes.size());
        for (int i = 0, m = fieldNames.size(); i < m; i++)
        {
            ByteBuffer fieldName = fieldNames.get(i);
            try
            {
                String stringFieldName = ByteBufferUtil.string(fieldName, StandardCharsets.UTF_8);
                stringFieldNames.add(stringFieldName);
                fieldSerializers.put(stringFieldName, fieldTypes.get(i).getSerializer());
            }
            catch (CharacterCodingException ex)
            {
                throw new AssertionError("Got non-UTF8 field name for user-defined type: " + ByteBufferUtil.bytesToHex(fieldName), ex);
            }
        }
        this.serializer = new UserTypeSerializer(fieldSerializers);
    }

    public static UserType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> params = parser.getUserTypeParameters();
        String keyspace = params.left.left;
        ByteBuffer name = params.left.right;
        List<ByteBuffer> columnNames = new ArrayList<>(params.right.size());
        List<AbstractType<?>> columnTypes = new ArrayList<>(params.right.size());
        for (Pair<ByteBuffer, AbstractType> p : params.right)
        {
            columnNames.add(p.left);
            columnTypes.add(p.right.freeze());
        }
        return new UserType(keyspace, name, columnNames, columnTypes);
    }

    public AbstractType<?> fieldType(int i)
    {
        return type(i);
    }

    public List<AbstractType<?>> fieldTypes()
    {
        return types;
    }

    public ByteBuffer fieldName(int i)
    {
        return fieldNames.get(i);
    }

    public String fieldNameAsString(int i)
    {
        return stringFieldNames.get(i);
    }

    public List<ByteBuffer> fieldNames()
    {
        return fieldNames;
    }

    public String getNameAsString()
    {
        return UTF8Type.instance.compose(name);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<String, Object> map = (Map<String, Object>) parsed;

        Json.handleCaseSensitivity(map);

        List<Term> terms = new ArrayList<>(types.size());

        Set keys = map.keySet();
        assert keys.isEmpty() || keys.iterator().next() instanceof String;

        int foundValues = 0;
        for (int i = 0; i < types.size(); i++)
        {
            Object value = map.get(stringFieldNames.get(i));
            if (value == null)
            {
                terms.add(Constants.NULL_VALUE);
            }
            else
            {
                terms.add(types.get(i).fromJSONObject(value));
                foundValues += 1;
            }
        }

        // check for extra, unrecognized fields
        if (foundValues != map.size())
        {
            for (Object fieldName : keys)
            {
                if (!stringFieldNames.contains(fieldName))
                    throw new MarshalException(String.format(
                            "Unknown field '%s' in value of user defined type %s", fieldName, getNameAsString()));
            }
        }

        return new UserTypes.DelayedValue(this, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, int protocolVersion)
    {
        ByteBuffer[] buffers = split(buffer);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < types.size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            String name = stringFieldNames.get(i);
            if (!name.equals(name.toLowerCase(Locale.US)))
                name = "\"" + name + "\"";

            sb.append('"');
            sb.append(Json.quoteAsJsonString(name));
            sb.append("\": ");

            ByteBuffer valueBuffer = (i >= buffers.length) ? null : buffers[i];
            if (valueBuffer == null)
                sb.append("null");
            else
                sb.append(types.get(i).toJSONString(valueBuffer, protocolVersion));
        }
        return sb.append("}").toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(keyspace, name, fieldNames, types);
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof UserType))
            return false;

        UserType that = (UserType)o;
        return keyspace.equals(that.keyspace) && name.equals(that.name) && fieldNames.equals(that.fieldNames) && types.equals(that.types);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.UserDefined.create(this);
    }

    @Override
    public boolean referencesUserType(String userTypeName)
    {
        return getNameAsString().equals(userTypeName) ||
               fieldTypes().stream().anyMatch(f -> f.referencesUserType(userTypeName));
    }

    @Override
    public String toString()
    {
        return getClass().getName() + TypeParser.stringifyUserTypeParameters(keyspace, name, fieldNames, types);
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return serializer;
    }

    public boolean isTuple()
    {
        return false;
    }

    public boolean isUDT()
    {
        return true;
    }
}
