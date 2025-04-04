import 'dart:async';
import 'dart:io';

import 'package:fluent_query_builder/src/models/validator.dart';
import 'package:fluent_query_builder/src/query_executors/my_mysql_executor.dart';

import 'exceptions/illegal_argument_exception.dart';
import 'exceptions/not_implemented_exception.dart';
import 'fluent_model_base.dart';
import 'models/connection_info.dart';
import 'models/delete.dart';
import 'models/expression.dart';
import 'models/insert.dart';
import 'models/query_builder.dart';
import 'models/query_builder_options.dart';
import 'models/raw.dart';
import 'models/select.dart';
import 'models/update.dart';
import 'query_executors/postgre_sql_executor.dart';
import 'query_executors/query_executor.dart';

class DbLayer {
  late QueryExecutor executor;
  late QueryBuilder currentQuery;
  final List<Map<Type, Function>>? factories; // = <Type, Function>{};
  //ex: DiskCache<Agenda>(factories: {Agenda: (x) => Agenda.fromJson(x)});
  //{this.factory}
  DbLayer(this.connectionInfo, {this.factories}) {
    //currentQuery = Select(QueryBuilderOptions());
  }
  //late QueryBuilderOptions options;
  DBConnectionInfo connectionInfo;
  static const dynamic DEFAULT_NULL = [];

  Future<DbLayer> connect([DBConnectionInfo? connInfo]) async {
    if (connInfo != null) {
      connectionInfo = connInfo.getSettings();
    }

    var nOfProces = connectionInfo.setNumberOfProcessorsFromPlatform
        ? Platform.numberOfProcessors
        : connectionInfo.numberOfProcessors;

    if (connectionInfo.driver == ConnectionDriver.pgsql) {
      if (connectionInfo.numberOfProcessors > 1 && connectionInfo.usePool) {
        executor = PostgreSqlExecutorPool(nOfProces, connectionInfo);
      } else {
        executor = PostgreSqlExecutor(connectionInfo);
        await executor.open();
      }
    } else {
      if (connectionInfo.numberOfProcessors > 1 && connectionInfo.usePool) {
        executor = MyMySqlExecutorPool(
          maxConnections: nOfProces,
          connectionInfo: connectionInfo,
        );
      } else {
        executor = MyMySqlExecutor(connectionInfo: connectionInfo);
        await executor.open();
      }
    }

    return this;
  }

  QueryBuilderOptions get options => connectionInfo.getQueryOptions();

  /// Starts a new expression with the provided options.
  /// @param options Options to use for expression generation.
  /// @return Expression
  Expression expression() {
    return Expression(options);
  }

  /// Starts the SELECT-query chain with the provided options
  /// @param options Options to use for query generation.
  /// @return QueryBuilder
  QueryBuilder select() {
    return currentQuery = Select(
      options,
      execFunc: exec,
      firstFunc: first,
      firstAsMapFuncWithMeta: firstAsMapWithMeta,
      getAsMapFuncWithMeta: getAsMapWithMeta,
      getAsMapFunc: getAsMap,
      firstAsMapFunc: firstAsMap,
      fetchAllFunc: _fetchAll,
      fetchSingleFunc: _fetchSingle,
      countFunc: _count,
    );
  }

  /// Starts the UPDATE-query.
  /// @return QueryBuilder
  QueryBuilder update() {
    return currentQuery = Update(
      options,
      execFunc: exec,
      firstFunc: first,
      firstAsMapFuncWithMeta: firstAsMapWithMeta,
      getAsMapFuncWithMeta: getAsMapWithMeta,
      getAsMapFunc: getAsMap,
      firstAsMapFunc: firstAsMap,
      updateSingleFunc: _updateSingle,
    );
  }

  /// Starts the INSERT-query with the provided options.
  /// @return QueryBuilder
  QueryBuilder insert() {
    return currentQuery = Insert(options,
        execFunc: exec,
        firstFunc: first,
        firstAsMapFuncWithMeta: firstAsMapWithMeta,
        getAsMapFuncWithMeta: getAsMapWithMeta,
        getAsMapFunc: getAsMap,
        firstAsMapFunc: firstAsMap,
        putSingleFunc: putSingle);
  }

  /// Starts the INSERT-query with the provided options and return id
  /// @return QueryBuilder
  QueryBuilder insertGetId({String? defaultIdColName = 'id'}) {
    return currentQuery = Insert(options,
        returningFields: [defaultIdColName],
        execFunc: exec,
        firstFunc: first,
        firstAsMapFuncWithMeta: firstAsMapWithMeta,
        getAsMapFuncWithMeta: getAsMapWithMeta,
        getAsMapFunc: getAsMap,
        firstAsMapFunc: firstAsMap,
        putSingleFunc: putSingle);
  }

  /// Starts the INSERT-query with the provided options and return * or returningFields
  /// @return QueryBuilder
  QueryBuilder insertGetAll({List<String>? returningFields}) {
    return currentQuery = Insert(options,
        returningFields: returningFields = returningFields ?? ['*'],
        execFunc: exec,
        firstFunc: first,
        firstAsMapFuncWithMeta: firstAsMapWithMeta,
        getAsMapFuncWithMeta: getAsMapWithMeta,
        getAsMapFunc: getAsMap,
        firstAsMapFunc: firstAsMap,
        putSingleFunc: putSingle);
  }

  /// Starts the DELETE-query with the provided options.
  /// @return QueryBuilder
  QueryBuilder delete() {
    return currentQuery = Delete(
      options,
      execFunc: exec,
      firstFunc: first,
      firstAsMapFuncWithMeta: firstAsMapWithMeta,
      getAsMapFuncWithMeta: getAsMapWithMeta,
      getAsMapFunc: getAsMap,
      firstAsMapFunc: firstAsMap,
      deleteSingleFunc: _deleteSingle,
    );
  }

  ///function to execute query from raw SQL String
  QueryBuilder raw(String rawQueryString,
      {Map<String, dynamic>? substitutionValues}) {
    return currentQuery = Raw(
      rawQueryString,
      options,
      substitutionValues: substitutionValues,
      execFunc: exec,
      firstFunc: first,
      firstAsMapFuncWithMeta: firstAsMapWithMeta,
      getAsMapFuncWithMeta: getAsMapWithMeta,
      getAsMapFunc: getAsMap,
      firstAsMapFunc: firstAsMap,
      countFunc: _count,
    );
  }

  ///this method to execute current query and get results as List
  Future<List<List>> exec() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Is nessesary query');
    }

    final rows = await executor.query(
      currentQuery.toSql(),
      substitutionValues: currentQuery.buildSubstitutionValues(),
      returningFields: currentQuery.buildReturningFields(),
    );
    return rows;
  }

  //alias for exec o execute current query and get results as List
  Future<List<List>> get() async {
    return exec();
  }

  Future<List<Map<String, Map<String, dynamic>>>> getAsMapWithMeta() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Dblayer@getAsMapWithMeta Is nessesary query');
    }
    final rows = await executor.getAsMapWithMeta(currentQuery.toSql(),
        substitutionValues: currentQuery.buildSubstitutionValues());
    return rows;
  }

  Future<List?> first() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Dblayer@first Is nessesary query');
    }
    final rows = await executor.query(currentQuery.toSql(isFirst: true),
        substitutionValues: currentQuery.buildSubstitutionValues());

    if (rows.isNotEmpty) {
      return rows[0];
    } else {
      return null;
    }
  }

  Future<int> _count() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Is nessesary query');
    }

    final rows = await executor.query(currentQuery.toSql(isCount: true),
        substitutionValues: currentQuery.buildSubstitutionValues());
    //total_records

    if (rows.isNotEmpty) {
      return rows[0][0];
    }

    return 0;
  }

  Future<Map<String, Map<String, dynamic>>?> firstAsMapWithMeta() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Dblayer@firstAsMapWithMeta Is nessesary query');
    }
    final rows = await getAsMapWithMeta();
    //if (rows != null) {
    if (rows.isNotEmpty) {
      return rows[0];
    } else {
      return null;
    }
    /* } else {
      return null;
    }*/
  }

  Future<List<Map<String, dynamic>>> getAsMap() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Dblayer@getAsMap Is nessesary query');
    }

    final rows = await executor.getAsMap(currentQuery.toSql(),
        substitutionValues: currentQuery.buildSubstitutionValues());
    return rows;
  }

  Future<Map<String, dynamic>?> firstAsMap() async {
    if (!currentQuery.isQuery()) {
      throw Exception('Dblayer@firstAsMap Is nessesary query');
    }

    final rows = await executor.getAsMap(currentQuery.toSql(isFirst: true),
        substitutionValues: currentQuery.buildSubstitutionValues());

    //if (rows != null) {
    if (rows.isNotEmpty) {
      return rows[0];
    } else {
      return null;
    }
    /*} else {
      return null;
    }*/
  }

  Future close() async {
    await executor.close();
  }

  Future<T?> transaction<T>(FutureOr<T> Function(DbLayer) f) {
    return executor.transaction<T>((queryExecutor) {
      var db = DbLayer(connectionInfo, factories: factories);
      db.executor = queryExecutor;
      return f(db);
    });
  }

  Future<dynamic> transaction2(Future<dynamic> Function(DbLayer) queryBlock,
      {int? commitTimeoutInSeconds}) {
    return executor.transaction2((queryExecutor) async {
      var db = DbLayer(connectionInfo, factories: factories);
      db.executor = queryExecutor;
      await queryBlock(db);
    }, commitTimeoutInSeconds: commitTimeoutInSeconds);
  }

  Future<DbLayer> startTransaction() async {
    await executor.startTransaction();
    return this;
  }

  Future<DbLayer> commit() async {
    await executor.commit();
    return this;
  }

  Future<DbLayer> rollback() async {
    await executor.rollback();
    return this;
  }

  Future<dynamic> reconnectIfNecessary() {
    return executor.reconnectIfNecessary();
  }

  Future<bool> isConnect() {
    return executor.isConnect();
  }

  /// execute command on database
  Future<int> execute(String query,
      {Map<String, dynamic>? substitutionValues}) async {
    return executor.execute(query, substitutionValues: substitutionValues);
  }

  /// add " for pgsql or ` for mysql
  String putInQuotes(String value) {
    if (options.driver == ConnectionDriver.pgsql) {
      var field = '"$value"';
      if (value.contains('.')) {
        field = value.split('.').map((e) => '"$e"').join('.');
      }
      return field;
    } else {
      var field = '`$value`';
      if (value.contains('.')) {
        field = value.split('.').map((e) => '`$e`').join('.');
      }
      return field;
    }
  }

  /// for input table.field return @field for pgsql and ? for mysql
  String formatSubititutioValue(String value) {
    var field = value;
    if (value.contains('.')) {
      field = value.split('.').last;
    }
    if (options.driver == ConnectionDriver.pgsql) {
      return '@$field';
    } else {
      return '?';
    }
  }

  /// format value for no SQL Injection,
  /// is not garatid securyty
  String formatValue(Object? value) {
    return Validator.formatValue(value, options);
  }

  Future<List<T>> _fetchAll<T>(
      [T Function(Map<String, dynamic>)? factory]) async {
    Function? fac;
    if (factories != null) {
      for (var item in factories!) {
        if (item.containsKey(T)) {
          fac = item[T];
        }
      }
    }

    fac ??= factory;

    if (fac == null) {
      throw IllegalArgumentException('Dblayer@fetchAll factory not defined');
    }

    var records = await getAsMap();

    final list = <T>[];

    if (records.isNotEmpty) {
      for (var item in records) {
        list.add(fac(item));
      }
      // if is Relations
      //get relations data
      var ormDefinitions = _validateModel(fac(records[0]));
      if (ormDefinitions.isRelations()) {
        var len = ormDefinitions.relations!.length;
        for (var i = 0; i < len; i++) {
          var relation = ormDefinitions.relations![i];

          records = await getRelationFromMaps(
            records,
            relation.tableRelation,
            relation.localKey,
            relation.foreignKey,
            defaultNull: null,
          );
        }
      }
      //print('records $records');
    }

    return list;
  }

  Future<T?> _fetchSingle<T>(
      [T Function(Map<String?, dynamic>)? factory]) async {
    Function? fac;
    if (factories != null) {
      for (var item in factories!) {
        if (item.containsKey(T)) {
          fac = item[T];
        }
      }
    }

    fac ??= factory;

    if (fac == null) {
      throw IllegalArgumentException('Dblayer@fetchAll factory not defined');
    }
    final record = await firstAsMap();

    if (record != null) {
      return fac(record);
    }
    return null;
  }

  Future putSingle<T>(T entity) async {
    if (entity == null) {
      throw IllegalArgumentException('Dblayer@putSingle entity not defined');
    }

    var ormDefinitions = _validateModel(entity);
    var query = insert();
    query.setAll(ormDefinitions.data);
    query.into(ormDefinitions.tableName);
    return query.exec();
  }

  Future putSingleGetId<T>(T entity) async {
    if (entity == null) {
      throw IllegalArgumentException('Dblayer@putSingle entity not defined');
    }

    var ormDefinitions = _validateModel(entity);

    var id, query;
    var mainInsertData = ormDefinitions.data;
    // if is Relations
    if (ormDefinitions.isRelations()) {
      var len = ormDefinitions.relations!.length;
      for (var i = 0; i < len; i++) {
        var relation = ormDefinitions.relations![i];

        if (relation.data.isNotEmpty) {
          query = insertGetId(defaultIdColName: relation.localKey)
              .setAll(relation.data)
              .into(relation.tableRelation);
          id = (await query.exec())[0][0];
          mainInsertData[relation.foreignKey] = id;
        }
      }

      query = insertGetId(defaultIdColName: ormDefinitions.primaryKey)
          .setAll(mainInsertData)
          .into(ormDefinitions.tableName);

      id = (await query.exec())[0][0];
    } else {
      var query = insertGetId(defaultIdColName: ormDefinitions.primaryKey)
          .setAll(mainInsertData)
          .into(ormDefinitions.tableName);
      id = (await query.exec())[0][0];
    }

    return id;
  }

  Future _updateSingle<T>(T entity, [QueryBuilder? queryBuilder]) async {
    if (queryBuilder == null) {
      throw IllegalArgumentException(
          'Dblayer@updateSingle queryBuilder not defined');
    }

    var ormDefinitions = _validateModel(entity);
    queryBuilder.table(ormDefinitions.tableName);
    queryBuilder.setAll(ormDefinitions.data);
    await queryBuilder.exec();

    if (ormDefinitions.isRelations()) {}
  }

  Future _deleteSingle<T>(T entity, [QueryBuilder? queryBuilder]) async {
    if (queryBuilder == null) {
      throw IllegalArgumentException(
          'Dblayer@_deleteSingle queryBuilder not defined');
    }

    var ormDefinitions = _validateModel(entity);
    queryBuilder.from(ormDefinitions.tableName);
    queryBuilder.whereSafe(
        '${ormDefinitions.primaryKey}', '=', ormDefinitions.primaryKeyVal);
    await exec();
  }

  ///this method validate and ch model
  OrmDefinitions _validateModel(entity) {
    if (entity == null) {
      throw Exception('Dblayer@_validateModel cannot be null');
    }
    /*try {
      var model = entity as FluentModelBase;
    } catch (e) {
      throw NotImplementedException('entity has not implemented the FluentModelBase interface');
    }*/
    if (!(entity is FluentModelBase)) {
      throw NotImplementedException(
          'entity has not implemented the FluentModelBase interface');
    }

    // ignore: unnecessary_cast
    var model = entity as FluentModelBase;

    var primaryKey = model.ormDefinitions.primaryKey;

    var data = model.toMap();

    /*if (data == null) {
      throw IllegalArgumentException('toMap() cannot return null');
    }*/

    var primaryKeyVal;
    data.forEach((key, val) {
      if (key == primaryKey) {
        primaryKeyVal = val;
      }
    });

    if (model.ormDefinitions.fillable?.isNotEmpty == true &&
        model.ormDefinitions.guarded?.isNotEmpty == true) {
      throw IllegalArgumentException(
          'Importantly, you should use either fillable or guarded - not both.');
    }

    var newData = <String, dynamic>{};

    if (model.ormDefinitions.fillable?.isNotEmpty == true) {
      data.forEach((key, val) {
        model.ormDefinitions.fillable!.forEach((item) {
          if (key == item) {
            newData[key] = val;
          }
        });
      });
      data = newData;
    }

    if (model.ormDefinitions.guarded?.isNotEmpty == true) {
      data.forEach((key, val) {
        model.ormDefinitions.guarded!.forEach((item) {
          if (key != item) {
            newData[key] = val;
          }
        });
      });
      data = newData;
    }

    var newRelations = <OrmRelation>[];
    //seta os dados da relação e remove dos dados do insert principal
    if (model.ormDefinitions.isRelations()) {
      var len = model.ormDefinitions.relations!.length;
      for (var i = 0; i < len; i++) {
        var relation = model.ormDefinitions.relations![i];
        relation.data = data[relation.relationName];
        newRelations.add(relation);
        data.remove(relation.relationName);
      }
    }

    var ormDefinitions = model.ormDefinitions.clone();
    ormDefinitions.data = data;
    ormDefinitions.primaryKeyVal = primaryKeyVal;
    ormDefinitions.relations = newRelations;

    return ormDefinitions;
  }

  ///
  /// @param data
  /// @param tableName nome da tabela relacionada
  /// @param localKey key id da tabela relacionada
  /// @param foreignKey id contido nos dados passados pelo parametro data para comparar com o key id da tabela relacionada
  /// @param relationName nome da chave no map que estara com o resultado
  /// @param defaultNull valor padrão para a chave no map caso não tenha resultado List | null
  ///
  /// @param null callback_fields
  /// Este parametro deve ser uma função anonima com um parametro que é o campo
  /// utilizada para alterar as informações de um determinado campo vindo do banco
  /// Exemplo:
  /// (field) {
  ///  field['description'] = strip_tags(field['description']);
  /// }
  ///
  /// @param null $callback_query
  /// Este parametro deve ser uma função com um parametro. Neste parametro você
  /// receberá a query utilizada na consulta, possibilitando
  /// realizar operações de querys extras para esta ação.
  ///
  /// Exemplo:
  /// (query) {
  ///  query.orderBy('field_name', 'asc');
  /// }
  ///
  /// @param bool isSingle
  ///
  ///
  Future<List<Map<String, dynamic>>> getRelationFromMaps(
    List<Map<String, dynamic>> data,
    String tableName,
    String localKey,
    String foreignKey, {
    String? relationName,
    dynamic defaultNull = DEFAULT_NULL,
    Function(Map<String?, dynamic>)? callback_fields,
    Function(QueryBuilder)? callback_query,
    isSingle = false,
  }) async {
    //1º obtem os ids
    var itens_id = <dynamic>[];
    for (var map in data) {
      var itemId = map.containsKey(foreignKey) ? map[foreignKey] : null;

      //não adiciona se for nulo ou vazio ou diferente de int
      if (itemId != null) {
        itens_id.add(itemId);
      }
    }

    //instancia o objeto query builder
    var query = select().from(tableName);
    //checa se foi passado callback_query para mudar a query
    if (callback_query != null) {
      callback_query(query);
    }

    List<Map<String?, dynamic>>? queryResult;
    //se ouver itens a serem pegos no banco
    if (itens_id.isNotEmpty) {
      //prepara a query where in e executa
      query.whereRaw(
          '"$tableName"."$localKey" in (${itens_id.map((e) => "'$e'").join(",")})');
      queryResult = await query.getAsMap();
    } else {
      queryResult = null;
    }

    //verifica se foi passado um nome para o node de resultados
    if (relationName != null) {
      relationName = relationName + '';
    } else {
      relationName = tableName;
    }
    if (isSingle) {
      defaultNull = null;
    }

    //var result = <Map<String, dynamic>>[];
    //intera sobre a lista de dados passados
    for (var item in data) {
      //result.add({relationName: defaultNull});
      item[relationName] = defaultNull;
      var conjunto = [];
      //faz o loop sobre os resultados da query
      if (queryResult != null) {
        for (Map<String?, dynamic>? value in queryResult) {
          if (value is Map<String, dynamic>) {
            //verifica se o item corrente tem relação com algum filho trazido pela query
            if (item[foreignKey] == value[localKey]) {
              //checa se foi passado callback_fields
              if (callback_fields != null) {
                value = callback_fields(value);
              }
              //verifica se é para trazer um filho ou varios
              if (isSingle) {
                item[relationName] = value ?? defaultNull;
                break;
              } else {
                conjunto.add(value ?? defaultNull);
              }

              item[relationName] = conjunto;
            }
          }
        }
      }
    }

    //fim
    return data;
  }
}
