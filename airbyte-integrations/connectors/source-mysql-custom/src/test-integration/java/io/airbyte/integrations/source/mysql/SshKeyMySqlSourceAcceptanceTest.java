/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mysqlcustomsql;

import java.nio.file.Path;

public class SshKeyMySqlSourceAcceptanceTest extends AbstractSshMySqlSourceAcceptanceTest {

  @Override
  public Path getConfigFilePath() {
    return Path.of("secrets/ssh-key-config.json");
  }

}
