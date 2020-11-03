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
package com.facebook.presto.udf;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.HiveTicketCache;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.ClientColumnEncryptionUtils;

import java.security.AccessControlException;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;

public final class SecurityFunctions
{
    private SecurityFunctions() {}

    public static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    @Description("Decrypts the string using key")
    @ScalarFunction("decryptUDF")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice decryptString(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice v1)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(HivePlugin.class.getClassLoader())) {
            HiveTicketCache hiveTicketCache = new HiveTicketCache();
            String v1str = v1.toStringUtf8();
            HdfsEnvironment hdfsEnvironment = hiveTicketCache.getHdfsEnvironment();
            if (hdfsEnvironment == null) {
                throw new RuntimeException("hdfsEnvironment not initialized yet");
            }
            StringBuilder result = new StringBuilder();
            try {
                hdfsEnvironment.getHdfsAuthentication().doAs(session.getUser(), () -> {
                    String decryptedString = "";
                    try {
                        ClientColumnEncryptionUtils c = new ClientColumnEncryptionUtils(hdfsEnvironment.getBaseConfiguration());
                        decryptedString = c.decrypt(v1str);
                    }
                    catch (AccessControlException e) {
                        throw new PrestoException(PERMISSION_DENIED, "User does not have permission.");
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    result.append(decryptedString);
                });
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Slices.utf8Slice(result.toString());
        }
    }

    @Description("Encrypts the string using key")
    @ScalarFunction("encryptUDF")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice encryptString(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice v1, @SqlType(StandardTypes.VARCHAR) Slice v2)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(HivePlugin.class.getClassLoader())) {
            HiveTicketCache hiveTicketCache = new HiveTicketCache();
            String v1str = v1.toStringUtf8();
            String v2str = v2.toStringUtf8();
            HdfsEnvironment hdfsEnvironment = hiveTicketCache.getHdfsEnvironment();
            if (hdfsEnvironment == null) {
                throw new RuntimeException("hdfsEnvironment not initialized yet");
            }
            StringBuilder result = new StringBuilder();
            try {
                hdfsEnvironment.getHdfsAuthentication().doAs(session.getUser(), () -> {
                    String encryptedString = "";
                    try {
                        ClientColumnEncryptionUtils c = new ClientColumnEncryptionUtils(hdfsEnvironment.getBaseConfiguration());
                        encryptedString = c.encrypt(v1str, v2str);
                    }
                    catch (AccessControlException e) {
                        throw new PrestoException(PERMISSION_DENIED, "User does not have permission.");
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    result.append(encryptedString);
                });
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Slices.utf8Slice(result.toString());
        }
    }

    @SqlNullable
    @Description("Check if the string is encrypted or not")
    @ScalarFunction("isEncrypted")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean isEncrypted(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice v1)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(HivePlugin.class.getClassLoader())) {
            HiveTicketCache hiveTicketCache = new HiveTicketCache();
            String v1str = v1.toStringUtf8();
            HdfsEnvironment hdfsEnvironment = hiveTicketCache.getHdfsEnvironment();
            if (hdfsEnvironment == null) {
                throw new RuntimeException("hdfsEnvironment not initialized yet");
            }
            try {
                return hdfsEnvironment.getHdfsAuthentication().doAs(session.getUser(), () -> {
                    try {
                        ClientColumnEncryptionUtils c = new ClientColumnEncryptionUtils(hdfsEnvironment.getBaseConfiguration());
                        return c.isEncrypted(v1str);
                    }
                    catch (AccessControlException e) {
                        throw new PrestoException(PERMISSION_DENIED, "User does not have permission.");
                    }
                    catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
