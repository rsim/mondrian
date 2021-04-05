/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016 - 2017 Hitachi Vantara
// All Rights Reserved.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import mondrian.olap.Util;
import mondrian.spi.Dialect.DatabaseProduct;

public class MariaDBDialect extends MySqlDialect {

  public static final JdbcDialectFactory FACTORY =
      new JdbcDialectFactory(
          MariaDBDialect.class,
          DatabaseProduct.MARIADB)
      {
          @Override
          protected boolean acceptsConnection(Connection connection) {
              return super.acceptsConnection(connection);
          }
      };


  public MariaDBDialect( Connection connection ) throws SQLException {
    super( connection );
  }

  @Override
  protected String deduceProductName(DatabaseMetaData databaseMetaData) {
      // It is possible for someone to use the MariaDB JDBC driver with Infobright . . .
      final String productName = super.deduceProductName(databaseMetaData);
      if (isInfobright(databaseMetaData)) {
          return "MySQL (Infobright)";
      }
      return productName;
  }

  // PATCH: MariaDB ColumnStore does not support selecting columns that are not in group by
  @Override
  protected boolean deduceSupportsSelectNotInGroupBy(Connection connection) throws SQLException {
      return false;
  }

  // PATCH: MariaDB ColumnStore does not support compound count distinct, see https://jira.mariadb.org/browse/MCOL-3738
  @Override
  public boolean allowsCompoundCountDistinct() {
      return false;
  }

}
