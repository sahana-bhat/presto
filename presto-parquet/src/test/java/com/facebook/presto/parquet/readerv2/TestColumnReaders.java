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
package com.facebook.presto.parquet.readerv2;

import com.facebook.presto.parquet.BenchmarkParquetReader;
import org.testng.annotations.Test;

public class TestColumnReaders
{
    @Test
    public void columnReaders()
    {
        // Test end-2-end column reader using the BenchmarkParquetReader, it already contains
        // the code to generate parquet files and read.
        BenchmarkParquetReader benchmarkParquetReader = new BenchmarkParquetReader();
    }
}
