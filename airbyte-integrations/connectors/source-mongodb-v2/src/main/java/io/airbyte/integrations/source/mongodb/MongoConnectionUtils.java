/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import static io.airbyte.integrations.source.mongodb.MongoConstants.DRIVER_NAME;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.airbyte.integrations.source.mongodb.cdc.MongoDbDebeziumPropertiesManager;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper utility for building a {@link MongoClient}.
 */
public class MongoConnectionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoConnectionUtils.class);

  /**
   * Creates a new {@link MongoClient} from the source configuration.
   *
   * @param config The source's configuration.
   * @return The configured {@link MongoClient}.
   */
  public static MongoClient createMongoClient(final MongoDbSourceConfig config) {
    final ConnectionString mongoConnectionString = new ConnectionString(buildConnectionString(config));

    final MongoDriverInformation mongoDriverInformation = MongoDriverInformation.builder()
        .driverName(DRIVER_NAME)
        .build();

    final MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
        .applyConnectionString(mongoConnectionString);

    // Enable wire protocol compression if not already set in connection string.
    // zstd provides best compression ratio for large document transfers (50-70% reduction).
    // Falls back to snappy and zlib in case zstd is not available on the server.
    if (mongoConnectionString.getCompressorList() == null || mongoConnectionString.getCompressorList().isEmpty()) {
      mongoClientSettingsBuilder.compressorList(List.of(
          MongoCompressor.createZstdCompressor(),
          MongoCompressor.createSnappyCompressor(),
          MongoCompressor.createZlibCompressor()));
      LOGGER.info("Wire protocol compression enabled (zstd, snappy, zlib)");
    }

    if (mongoConnectionString.getReadPreference() == null) {
      mongoClientSettingsBuilder.readPreference(ReadPreference.secondaryPreferred());
    }

    if (config.hasAuthCredentials()) {
      final String authSource = config.getAuthSource();
      final String user = URLEncoder.encode(config.getUsername(), StandardCharsets.UTF_8);
      final String password = config.getPassword();
      mongoClientSettingsBuilder.credential(MongoCredential.createCredential(user, authSource, password.toCharArray()));
    }

    return MongoClients.create(mongoClientSettingsBuilder.build(), mongoDriverInformation);
  }

  private static String buildConnectionString(final MongoDbSourceConfig config) {
    return MongoDbDebeziumPropertiesManager.buildConnectionString(config.getDatabaseConfig());
  }

}
