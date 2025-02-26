import 'dart:async';

import 'package:logging/logging.dart';
import 'package:mysql_client/mysql_client.dart';

import '../../fluent_query_builder.dart';
import 'query_executor.dart';
import 'utils.dart';

class MyMySqlExecutor extends QueryExecutor<MySQLConnection> {
  /// An optional [Logger] to write to.
  final Logger? logger;
  @override
  late MySQLConnection? connection;
  DBConnectionInfo? connectionInfo;

  MyMySqlExecutor({this.logger, this.connectionInfo, this.connection});

  @override
  Future<void> open() async {
    connection = await MySQLConnection.createConnection(
      host: connectionInfo!.host,
      port: connectionInfo!.port,
      userName: connectionInfo!.username,
      password: connectionInfo!.password,
      databaseName: connectionInfo!.database,
      secure: connectionInfo!.useSSL,
    );
    await connection?.connect();
  }

  @override
  Future<dynamic> reconnectIfNecessary() async {
    try {
      await query('select true');
      return this;
    } catch (e) {
//when the database restarts there is a loss of connection
      if ('$e'.contains('Cannot write to socket, it is closed')) {
        await reconnect();
        return this;
      }
      rethrow;
    }
  }

  Future<void> reconnect() async {
    await open();
  }

  @override
  Future<void> close() {
    if (connection is MySQLConnection) {
      return (connection as MySQLConnection).close();
    } else {
      return Future.value();
    }
  }

  @override
  Future<int> execute(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    await connection!.execute(query);
    return 0;
  }

  ///this method run query on MySQL or MariaDB DataBase
  @override
  Future<List<List>> query(String query,
      {Map<String, dynamic>? substitutionValues,
      List<String?>? returningFields}) async {
    // Change @id -> ?
    // for (var name in substitutionValues.keys) {
    //   query = query.replaceAll('@$name', '?');
    // }
    //print('MySqlExecutor@query Query: $query');
    //print('MySqlExecutor@query Values: $substitutionValues');
    logger?.fine('MySqlExecutor@query Query: $query');
    logger?.fine('MySqlExecutor@query Values: $substitutionValues');
    // print('MySqlExecutor@query Query: $query');
    //  print('MySqlExecutor@query Values: $substitutionValues');
    //  print('MySqlExecutor@query Fields: $returningFields');

    /*
    for MariaDB 10.5 only
    if (returningFields != null) {
      var fields = returningFields.join(', ');
      var returning = 'RETURNING $fields';
      query = '$query $returning';
    }*/

    if (returningFields?.isNotEmpty != true) {
      IResultSet results;
      try {
        final stmt = await connection!.prepare(query);
        results = await stmt.execute(
          Utils.substitutionMapToList(substitutionValues),
        );
      } catch (e) {
        //reconnect in Error
        //MySQL Client Error: Connection cannot process a request for Instance of 'PrepareHandler' while a request is already in progress for Instance of 'PrepareHandler'
        if ('$e'.contains('PrepareHandler') ||
            '$e'.contains('Cannot write to socket, it is closed')) {
          //print('MySqlExecutor@query reconnect in Error');
          await reconnect();
          final stmt = await connection!.prepare(query);
          results = await stmt.execute(
            Utils.substitutionMapToList(substitutionValues),
          );
        } else {
          rethrow;
        }
      }
      return results.map((r) => r.toList()).toList();
    } else {
      return Future(() async {
        return connection!.transactional((tx) async {
          var tableName = '';
          /*          
          INSERT INTO `pessoas` (nome,telefone)  VALUES ('Dog','2771-2898') ;
          SELECT id,nome from `pessoas` WHERE id=LAST_INSERT_ID();
          */
          var indexOfInsert = query.toUpperCase().indexOf('INTO');
          var indexOfEnd = query.indexOf('(');
          tableName = query.substring(indexOfInsert + 4, indexOfEnd);

          IResultSet writeResults;
          try {
            final stmt = await tx.prepare(
              query,
            );
            writeResults = await stmt.execute(
              Utils.substitutionMapToList(substitutionValues),
            );
          } catch (e) {
            //reconnect in Error
            //MySQL Client Error: Connection cannot process a request for Instance of 'PrepareHandler' while a request is already in progress for Instance of 'PrepareHandler'
            if ('$e'.contains('PrepareHandler') ||
                '$e'.contains('Cannot write to socket, it is closed')) {
              //print('MySqlExecutor@query reconnect in Error');
              await reconnect();
              final stmt = await tx.prepare(
                query,
              );
              writeResults = await stmt.execute(
                Utils.substitutionMapToList(substitutionValues),
              );
            } else {
              rethrow;
            }
          }

          var fieldSet = returningFields!.map((s) => '`$s`').join(',');
          var fetchSql = 'select $fieldSet from $tableName where id = ?;';
          //print('fetchSql $fetchSql');
          //print('writeResults.insertId ${writeResults.insertId}');

          logger?.fine(fetchSql);
          IResultSet readResults;

          try {
            final stmt = await tx.prepare(
              fetchSql,
            );
            readResults = await stmt.execute(
              [writeResults.lastInsertID],
            );
          } catch (e) {
            //reconnect in Error
            //MySQL Client Error: Connection cannot process a request for Instance of 'PrepareHandler' while a request is already in progress for Instance of 'PrepareHandler'
            if ('$e'.contains('PrepareHandler') ||
                '$e'.contains('Cannot write to socket, it is closed')) {
              //print('MySqlExecutor@query reconnect in Error');
              await reconnect();
              final stmt = await tx.prepare(
                fetchSql,
              );
              readResults = await stmt.execute(
                [writeResults.lastInsertID],
              );
            } else {
              rethrow;
            }
          }

          var mapped = readResults.rows.map((e) => _getListValues(e)).toList();
          //print('mapped $mapped');

          return mapped;
        });
      });
    }
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> getAsMapWithMeta(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    // return rs.map((row) => row.toTableColumnMap()).toList();
    throw UnsupportedOperationException('mappedResultsQuery not implemented');
    //var rows = await this.query(query,substitutionValues);
  }

  @override
  Future<List<Map<String, dynamic>>> getAsMap(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    //print('MySqlExecutor@getAsMap query $query');
    //print('MySqlExecutor@getAsMap substitutionValues $substitutionValues');
    var results = <Map<String, dynamic>>[];

    for (var name in substitutionValues!.keys) {
      query = query.replaceAll('@$name', '?');
    }
    IResultSet result;
    try {
      final stmt = await connection!.prepare(query);
      result = await stmt.execute(
        Utils.substitutionMapToList(substitutionValues),
      );
    } catch (e) {
      //reconnect in Error
      //MySQL Client Error: Connection cannot process a request for Instance of 'PrepareHandler' while a request is already in progress for Instance of 'PrepareHandler'
      //Bad state: Cannot write to socket, it is closed
      //print('MySqlExecutor@getAsMap reconnect in Error $e');
      if ('$e'.contains('PrepareHandler') ||
          '$e'.contains('Cannot write to socket, it is closed')) {
        //print('MySqlExecutor@getAsMap reconnect in Error');
        await reconnect();
        final stmt = await connection!.prepare(query);
        result = await stmt.execute(
          Utils.substitutionMapToList(substitutionValues),
        );
      } else {
        rethrow;
      }
    }

    for (final row in result.rows) {
      results.add(row.typedAssoc());
    }
    //print('MySqlExecutor@getAsMap results ${results}');
    return results;
  }

  @override
  Future<T> transaction<T>(FutureOr<T> Function(QueryExecutor) f) async {
    return connection!.transactional(
      (tx) async {
        var executor = MyMySqlExecutor(
          logger: logger,
          connectionInfo: connectionInfo,
          connection: tx,
        );

        return await f(executor);
      },
    );
  }

  @override
  Future<dynamic> transaction2(
    Future<dynamic> Function(QueryExecutor) queryBlock, {
    int? commitTimeoutInSeconds,
  }) async {
    return connection!.transactional(
      (tx) async {
        var executor = MyMySqlExecutor(
          logger: logger,
          connectionInfo: connectionInfo,
          connection: tx,
        );

        return await queryBlock(executor);
      },
    );
  }

  @override
  Future<QueryExecutor> startTransaction() {
    throw UnsupportedOperationException('startTransaction not implemented');
  }

  @override
  Future<void> commit() async {
    return Future.value();
  }

  @override
  Future<void> rollback() async {
    return Future.value();
  }

  List _getListValues(ResultSetRow e) {
    return e.typedAssoc().values.toList();
  }
}

class MyMySqlExecutorPool extends QueryExecutor<MySQLConnectionPool> {
  /// An optional [Logger] to write to.
  final Logger? logger;

  @override
  MySQLConnectionPool? connection;
  DBConnectionInfo? connectionInfo;
  final int maxConnections;

  MyMySqlExecutorPool({
    this.logger,
    this.connectionInfo,
    this.maxConnections = 5,
  }) {
    connection = MySQLConnectionPool(
      host: connectionInfo!.host,
      port: connectionInfo!.port,
      userName: connectionInfo!.username,
      password: connectionInfo!.password,
      databaseName: connectionInfo!.database,
      secure: connectionInfo!.useSSL,
      maxConnections: maxConnections,
    );
  }

  @override
  Future<void> close() async {
    await connection?.close();
    connection = null;
  }

  @override
  Future<int> execute(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    await connection!.execute(query);
    return 0;
  }

  ///this method run query on MySQL or MariaDB DataBase
  @override
  Future<List<List>> query(String query,
      {Map<String, dynamic>? substitutionValues,
      List<String?>? returningFields}) async {
    // Change @id -> ?
    // for (var name in substitutionValues.keys) {
    //   query = query.replaceAll('@$name', '?');
    // }
    //print('MySqlExecutor@query Query: $query');
    //print('MySqlExecutor@query Values: $substitutionValues');
    logger?.fine('MySqlExecutor@query Query: $query');
    logger?.fine('MySqlExecutor@query Values: $substitutionValues');
    // print('MySqlExecutor@query Query: $query');
    //  print('MySqlExecutor@query Values: $substitutionValues');
    //  print('MySqlExecutor@query Fields: $returningFields');

    /*
    for MariaDB 10.5 only
    if (returningFields != null) {
      var fields = returningFields.join(', ');
      var returning = 'RETURNING $fields';
      query = '$query $returning';
    }*/

    if (returningFields?.isNotEmpty != true) {
      final stmt = await connection!.prepare(query);
      var results = await stmt.execute(
        Utils.substitutionMapToList(substitutionValues),
      );
      return results.map((r) => r.toList()).toList();
    } else {
      return Future(() async {
        return connection!.transactional((tx) async {
          /*          
          INSERT INTO `pessoas` (nome,telefone)  VALUES ('Dog','2771-2898') ;
          SELECT id,nome from `pessoas` WHERE id=LAST_INSERT_ID();
          */

          final stmt = await tx.prepare(
            query,
          );
          var writeResults = await stmt.execute(
            Utils.substitutionMapToList(substitutionValues),
          );

          var indexOfInsert = query.toUpperCase().indexOf('INTO');
          var indexOfEnd = query.indexOf('(');
          final tableName = query.substring(indexOfInsert + 4, indexOfEnd);

          var fieldSet = returningFields!.map((s) => '`$s`').join(',');
          var fetchSql = 'select $fieldSet from $tableName where id = ?;';

          logger?.fine(fetchSql);

          final stmtRead = await tx.prepare(
            fetchSql,
          );
          var readResults = await stmtRead.execute(
            [writeResults.lastInsertID],
          );

          var mapped = readResults.rows.map((e) => _getListValues(e)).toList();

          return mapped;
        });
      });
    }
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> getAsMapWithMeta(
    String query, {
    Map<String, dynamic>? substitutionValues,
  }) async {
    throw UnsupportedOperationException('mappedResultsQuery not implemented');
  }

  @override
  Future<List<Map<String, dynamic>>> getAsMap(
    String query, {
    Map<String, dynamic>? substitutionValues,
  }) async {
    var results = <Map<String, dynamic>>[];

    for (var name in substitutionValues!.keys) {
      query = query.replaceAll('@$name', '?');
    }

    final stmt = await connection!.prepare(query);
    var result = await stmt.execute(
      Utils.substitutionMapToList(substitutionValues),
    );

    for (final row in result.rows) {
      results.add(row.typedAssoc());
    }
    return results;
  }

  @override
  Future<T> transaction<T>(FutureOr<T> Function(QueryExecutor) f) async {
    return connection!.transactional(
      (tx) async {
        return await f(
          MyMySqlExecutor(
            logger: logger,
            connectionInfo: connectionInfo,
            connection: tx,
          ),
        );
      },
    );
  }

  @override
  Future<dynamic> transaction2(
    Future<dynamic> Function(QueryExecutor) queryBlock, {
    int? commitTimeoutInSeconds,
  }) async {
    return connection!.transactional(
      (tx) async {
        return await queryBlock(
          MyMySqlExecutor(
            logger: logger,
            connectionInfo: connectionInfo,
            connection: tx,
          ),
        );
      },
    );
  }

  @override
  Future<QueryExecutor> startTransaction() {
    throw UnsupportedOperationException('startTransaction not implemented');
  }

  @override
  Future<void> commit() async {
    throw UnsupportedOperationException('commit not implemented');
  }

  @override
  Future<void> rollback() async {
    throw UnsupportedOperationException('rollback not implemented');
  }

  List _getListValues(ResultSetRow e) {
    return e.typedAssoc().values.toList();
  }

  @override
  Future reconnectIfNecessary() {
    throw UnimplementedError();
  }
}
