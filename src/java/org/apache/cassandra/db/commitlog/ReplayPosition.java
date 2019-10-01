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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ReplayPosition implements Comparable<ReplayPosition>
{
    public static final ReplayPositionSerializer serializer = new ReplayPositionSerializer();

    // NONE is used for SSTables that are streamed from other nodes and thus have no relationship
    // with our local commitlog. The values satisfy the criteria that
    //  - no real commitlog segment will have the given id
    //  - it will sort before any real replayposition, so it will be effectively ignored by getReplayPosition
    public static final ReplayPosition NONE = new ReplayPosition(-1, 0);

    public final long segment;
    public final int position;

    public ReplayPosition(long segment, int position)
    {
        this.segment = segment;
        assert position >= 0;
        this.position = position;
    }

    public int compareTo(ReplayPosition that)
    {
        if (this.segment != that.segment)
            return Long.compare(this.segment, that.segment);

        return Integer.compare(this.position, that.position);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplayPosition that = (ReplayPosition) o;

        if (position != that.position) return false;
        return segment == that.segment;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (segment ^ (segment >>> 32));
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString()
    {
        return "ReplayPosition(" +
               "segmentId=" + segment +
               ", position=" + position +
               ')';
    }

    public ReplayPosition clone()
    {
        return new ReplayPosition(segment, position);
    }

    public static class ReplayPositionSerializer implements ISerializer<ReplayPosition>
    {
        public void serialize(ReplayPosition rp, DataOutputPlus out) throws IOException
        {
            out.writeLong(rp.segment);
            out.writeInt(rp.position);
        }

        public ReplayPosition deserialize(DataInputPlus in) throws IOException
        {
            return new ReplayPosition(in.readLong(), in.readInt());
        }

        public long serializedSize(ReplayPosition rp)
        {
            return TypeSizes.sizeof(rp.segment) + TypeSizes.sizeof(rp.position);
        }
    }
}
