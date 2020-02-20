/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.datagen;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Simple Datagenerator Source
 */
@Connector(
    name = "datagen",
    type = IOType.SOURCE,
    help = "A Simple Datagenerator Source",
    configClass = DataGenSourceConfig.class)
@Slf4j
public class DataGenSource extends PushSource<byte[]> {

    private ExecutorService executor;

    @Override
    public void open(final Map<String, Object> config,
                     final SourceContext sourceContext) throws Exception {
        final DataGenSourceConfig dataSourceConfig = DataGenSourceConfig.load(config);
        open(dataSourceConfig);

    }

    @VisibleForTesting
    public void open(final DataGenSourceConfig dataGenSourceConfig) {
        if (dataGenSourceConfig.getKeyword() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        executor = Executors.newSingleThreadExecutor();
        executor.submit(new DataGenerator(dataGenSourceConfig, this));
        log.info("datagen source started");
    }

    @Override
    public void close() {
        executor.shutdownNow();
        log.info("datagen source stopped");
    }

    private class DataGenerator implements Runnable {

        private final DataGenSourceConfig dataGenSourceConfig;
        private final DataGenSource dataGenSource;
        private final AtomicInteger counter;

        public DataGenerator(DataGenSourceConfig dataSourceConfig, DataGenSource dataGenSource) {
            this.dataGenSourceConfig = dataSourceConfig;
            this.dataGenSource = dataGenSource;
            this.counter = new AtomicInteger();
        }

        @SneakyThrows
        @Override
        public void run() {
            while (true) {
                dataGenSource.consume(new DataGenRecord(
                    Optional.ofNullable(""),
                    (dataGenSourceConfig.getKeyword() + "-" + counter.incrementAndGet()).getBytes()));
                Thread.sleep(50);
            }
        }
    }

    @Data
    static private class DataGenRecord implements Record<byte[]>, Serializable {
        private final Optional<String> key;
        private final byte[] value;
    }

}
