/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016 - 2021 Hitachi Vantara
// All Rights Reserved.
*/

package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.SQLException;

public class SingleStoreDialect extends MySqlDialect {

  public static final JdbcDialectFactory FACTORY =
      new JdbcDialectFactory(
          SingleStoreDialect.class,
          // While we're choosing dialects, this still looks like a MySQL connection.
          DatabaseProduct.MYSQL)
      {
          protected boolean acceptsConnection(Connection connection) {
              try {
                  return super.acceptsConnection(connection)
                      && isSingleStore(connection.getMetaData());
              } catch (SQLException e) {
                  throw Util.newError(
                      e, "Error while instantiating dialect");
              }
          }
      };

  public SingleStoreDialect(Connection connection) throws SQLException {
      super( connection );
  }

  public DatabaseProduct getDatabaseProduct() {
      return DatabaseProduct.SINGLESTORE;
  }

  @Override
  public boolean supportsMultiValueInExpr() {
      return false;
  }

  @Override
  protected boolean deduceSupportsSelectNotInGroupBy(Connection connection) throws SQLException {
      return false;
  }

}
