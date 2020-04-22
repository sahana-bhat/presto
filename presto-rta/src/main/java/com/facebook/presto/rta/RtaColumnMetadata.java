/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.rta;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;

public class RtaColumnMetadata
        extends ColumnMetadata
{
    // We need to preserve the case sensitivity of the column, store it here as the super class stores the value after lower-casing it
    private final String name;

    public RtaColumnMetadata(String name, Type type)
    {
        super(name, type);
        this.name = name;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .toString();
    }

    @Override
    public String getName()
    {
        return name.toLowerCase(ENGLISH);
    }

    public String getRtaName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, getType(), getComment(), isHidden());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RtaColumnMetadata other = (RtaColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.getType(), other.getType()) &&
                Objects.equals(this.getComment(), other.getComment()) &&
                Objects.equals(this.isHidden(), other.isHidden());
    }
}
