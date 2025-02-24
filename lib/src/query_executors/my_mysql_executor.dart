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
              query,
            );
            readResults = await stmt.execute(
              [writeResults.affectedRows],
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
              readResults = await stmt.execute(
                [writeResults.affectedRows],
              );
            } else {
              rethrow;
            }
          }

          var mapped = readResults.map((r) => r.toList()).toList();
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

    var fields = result.cols;
    for (final row in result.rows) {
      var map = <String, dynamic>{};
      //print('key: ${fields[0].name}, value: ${row[0]}');
      for (var i = 0; i < result.length; i++) {
        map.addAll({fields.elementAt(i).name: row.colAt(i)});
      }
      results.add(map);
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
      Future<dynamic> Function(QueryExecutor) queryBlock,
      {int? commitTimeoutInSeconds}) async {
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
}

// /// A [QueryExecutor] that manages a pool of PostgreSQL connections.
// class MySqlExecutorPool extends QueryExecutor<MySqlExecutor> {
//   /// The maximum amount of concurrent connections.
//   final int size;

//   /// Creates a new [PostgreSQLConnection], on demand.
//   ///
//   /// The created connection should **not** be open.
//   //final Querier Function() connectionFactory;
//   //final Future<Querier> Function() connectionFactory;

//   /// An optional [Logger] to print information to.
//   final Logger? logger;

//   @override
//   final List<MySqlExecutor> connections = [];

//   int _index = 0;
//   final Pool _pool, _connMutex = Pool(1);
//   DBConnectionInfo? connectionInfo;

//   MySqlExecutorPool(this.size, {this.logger, this.connectionInfo})
//       : _pool = Pool(size) {
//     assert(size > 0, 'Connection pool cannot be empty.');
//   }

//   /// Closes all connections.
//   @override
//   Future close() async {
//     await _pool.close();
//     await _connMutex.close();
//     return Future.wait(connections.map((c) => c.close()));
//   }

//   Future _open() async {
//     if (connections.isEmpty) {
//       final listCon = await Future.wait(
//         List.generate(size, (_) async {
//           logger?.fine('Spawning connections...');
//           //   await connectionFactory();

//           final executor =
//               MySqlExecutor(logger: logger, connectionInfo: connectionInfo);
//           await executor.open();

//           return executor;
//         }),
//       );
//       connections.addAll(listCon);
//     }
//   }

//   Future<MySqlExecutor> _next() {
//     return _connMutex.withResource(() async {
//       await _open();
//       if (_index >= size) _index = 0;
//       return connections[_index++];
//     });
//   }

//   @override
//   Future<List<Map<String, Map<String, dynamic>>>> getAsMapWithMeta(String query,
//       {Map<String, dynamic>? substitutionValues}) {
//     /*return _pool.withResource(() async {
//       final executor = await _next();
//       return executor.mappedResultsQuery(query, substitutionValues: substitutionValues);
//     });*/
//     throw UnsupportedOperationException('mappedResultsQuery not implemented');
//   }

//   @override
//   Future<List<Map<String, dynamic>>> getAsMap(String query,
//       {Map<String, dynamic>? substitutionValues}) async {
//     return _pool.withResource(() async {
//       final executor = await _next();
//       return executor.getAsMap(query, substitutionValues: substitutionValues);
//     });
//   }

//   @override
//   Future<int> execute(String query,
//       {Map<String, dynamic>? substitutionValues}) {
//     return _pool.withResource(() async {
//       final executor = await _next();
//       return executor.execute(query, substitutionValues: substitutionValues!);
//     });
//   }

//   @override
//   Future<List<List>> query(String query,
//       {Map<String, dynamic>? substitutionValues,
//       List<String?>? returningFields}) {
//     return _pool.withResource(() async {
//       final executor = await _next();
//       return executor.query(query,
//           substitutionValues: substitutionValues,
//           returningFields: returningFields);
//     });
//   }

//   @override
//   Future<T> transaction<T>(FutureOr<T> Function(QueryExecutor) f) {
//     return _pool.withResource(() async {
//       var executor = await _next();
//       return executor.transaction(f);
//     });
//   }

//   @override
//   Future<dynamic> transaction2(
//       Future<dynamic> Function(QueryExecutor) queryBlock,
//       {int? commitTimeoutInSeconds}) async {
//     return _pool.withResource(() async {
//       var executor = await _next();
//       return executor.transaction2(queryBlock);
//     });
//   }

//   @override
//   Future reconnectIfNecessary() {
//     throw UnimplementedError();
//   }
// }
