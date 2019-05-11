<?php

// TODO: add import/importAll function
/**
 * Class designed to ease the exportion and importion from and to SQLite and MySqli
 *
 * <p>
 * <b>Usage Example:</b>
 * <i><pre>
 * $SQLiteMySqli = new SQLiteMySqli();
 * $SQLiteMySqli->setMySQLi( getMySqliInstance() );
 * $SQLiteMySqli->setSQLite( "filePathSQLiteMySqli.db" );
 * $SQLiteMySqli->setDatabaseName( "exampleDatabase" );
 *
 * $result = $SQLiteMySqli->exportTableSQLiteMySqli( "students" );
 *
 * if ($result >= 0) echo "$result exported rows";
 * else throw $SQLiteMySqli->getLastError();</pre></i>
 *
 * @author aidn5
 * @version 1.1
 *         
 */
class SQLiteMySqli {

	// TODO: Add the code to detect and overwrite the data
	/**
	 * When fetching the data and a column has unique in its type and there is duplicate.
	 * Should it deletes the existed one and insert the new one?
	 * TRUE for yes. FALSE for no
	 *
	 * <p><b>Right now it does NOT work</b></p>
	 *
	 * @var boolean
	 */
	public $overWriteDataIfExist = false;

	// TODO: Add the code to detect and rewrite the table
	/**
	 * If the destination the table exists and holds data.
	 * Should the program alter the table/add columns or should it leave it as it is?
	 * TRUE for yes. FALSE for no
	 *
	 * <p><b>Right now it does NOT work</b></p>
	 *
	 * @var boolean
	 */
	public $RewriteTableifMust = false;

	// TODO: Add the code to detect the foreign key
	/**
	 * If the source table has foreign key.
	 * should the program ignore it or copy both tables?
	 * TRUE to ignore. FALSE to copy them both
	 *
	 * <p><b>Right now it does NOT work</b></p>
	 *
	 * @var boolean
	 */
	public $ignoreForeignKey = true;

	/**
	 * Do not insert the column with the primary key to the table
	 * Sometimes rows have the same primary key.
	 * It will throw an error upon trying to insert the duplicate.
	 * Enabling this makes the program ignore the primary key.
	 * <p>Useful when the primary key can be ignored</p>
	 *
	 * @var boolean
	 */
	public $ignorePrimaryKey = false;

	/**
	 * On inserting the data into the database.
	 * Should the program continue and ignore the rows which couldnt inserts?
	 *
	 * @var boolean
	 */
	public $ignoreErrors = false;

	/**
	 * Either the path to the location of the SQLite or the connection
	 *
	 * @var SQLite3|String
	 */
	protected $SQLite;

	/**
	 * Connection to the database
	 *
	 * @var mySQLi
	 */
	protected $mySQLi;

	/**
	 * The program uses transactions to transfer data faster and safer.
	 * To avoid using "BEGIN;" a name is provided
	 * to not interfere with the user's current transaction.
	 *
	 * @var String the Transaction's name
	 */
	protected $transactionName = "SQLiteMySqli";

	/**
	 * Due to the slowness of "information_schema.COLUMNS" table,
	 * which is used by {@link SQLiteMySqli::getColumnsDataFromMySQLi()},
	 * a database-name is used to speed up the process.
	 * by using it for "TABLE_SCHEMA" to avoid data directory scan.
	 *
	 * Besides: If the requested table's name is used in more than one database,
	 * all the keys will be added to the destination table upon creating it.
	 * E.g. table's name "statistcs". This name is also used by the database itself.
	 *
	 * <i>* NULL to use {@link SQLiteMySqli::getSelectedDatabase()}.
	 * "" (empty) to NOT use anything. String to use the string as the name</i>
	 *
	 * @var String database's name
	 *     
	 * @see SQLiteMySqli::setDatabaseName()
	 * @see SQLiteMySqli::getColumnsDataFromMySQLi()
	 * @link https://dev.mysql.com/doc/refman/5.5/en/information-schema-optimization.html
	 */
	protected $databaseName = null;

	/**
	 *
	 * Due to the slowness of "information_schema.COLUMNS" table,
	 * which is used by {@link SQLiteMySqli::getColumnsDataFromMySQLi()},
	 * Columns schema are cached here to speed up the process
	 *
	 * @var array
	 * @see SQLiteMySqli::getColumnsDataFromMySQLi()
	 */
	protected $cachedColumnsSchema = [];
	/**
	 * Last saved error occurred on: the lib, SQLite or Mysqli.
	 * <p>
	 * Due to the usage of transactions, Errors are replaced with success,
	 * when rolling back a transaction after failure.
	 *
	 * @var Exception
	 */
	protected $lastError = null;

	/**
	 * set the used database's name <i>to speed up the process</i> AND <i>evade some unnoticeable errors</i>
	 *
	 * @param String $databaseName
	 *        	the current used database's name. or NULL to auto select the current selected one. or (empty) to NOT use any
	 *        	
	 * @return self
	 *
	 * @see SQLiteMySqli::$databaseName
	 *
	 * @throws InvalidArgumentException if $databaseName does not meet all the specification
	 */
	public function setDatabaseName(String $databaseName): self {
		if ($databaseName !== null || strlen( $databaseName ) !== 0) $this->checkNameOrThrowError( $databaseName, "databaseName" );

		$this->databaseName = $databaseName;
		return $this;
	}

	/**
	 * Create a transaction to speed up the proccess of inserting data.
	 * It is also used as a backup-plan to ROLLBACK,
	 * when an error occure while inserting data.
	 *
	 * <p>
	 * <i><b>If the the transaction name is NULL,
	 * a transaction won't be created and the inserting will be direct!<b/></i>
	 *
	 * @param String|NULL $transationName
	 *        	Name of the transaction OR NULL/empty to disable it
	 *        	
	 * @return self
	 *
	 * @throws InvalidArgumentException if $name does not meet all the specification
	 */
	public function setTransactionName(String $transationName): self {
		if (strlen( $transationName ) === 0) $transationName = null;
		else $this->checkNameOrThrowError( $transationName, "databaseName" );

		// TODO: Security: Poeple can execute commands directly to the database after the begin and end of a transactions
		// To start transaction "BEGIN [$name]" and "COMMIT [$name]"... same with ROLLBACK too!
		// This can be ignored due to the user is the self programmer
		// and no one elses will need to change the name of the transaction
		$this->transactionName = $transationName;

		return $this;
	}

	/**
	 * get the SQLite3 connection
	 *
	 * @return SQLite3
	 */
	public function getSQLite(): SQLite3 {
		if (is_string( $this->SQLite )) {
			$this->SQLite = new SQLite3( $this->SQLite );
			return $this->SQLite;
		}
		else if (! ($this->SQLite instanceof SQLite3)) {
			throw new RuntimeException( "SQLite3 is not set. You can set it with setSQLite(...)" );
		}
		return $this->SQLite;
	}

	/**
	 *
	 * @param String|SQLite3|NULL $SQLite
	 *        	Either the path as <b>String</b> to the location
	 *        	OR <b>SQLite3</b> connection
	 *        	
	 * @return self
	 */
	public function setSQLite($SQLite): self {
		$this->SQLite = $SQLite;
		return $this;
	}


	/**
	 * get the MySQL connection
	 *
	 * @return mySQLi
	 */
	public function getMySQLi(): mySQLi {
		if (! ($this->mySQLi instanceof mySQLi)) {
			throw new RuntimeException( "mysqli is not set. You can set it with setMySQLi(...)" );
		}

		return $this->mySQLi;
	}

	/**
	 *
	 * @param mySQLi|NULL $mySQLi
	 *        	The connection to the desired database
	 *        	OR <b>null</b> to initate connection from the given settings
	 *        	
	 * @return self
	 */
	public function setMySQLi(mySQLi $mySQLi): Self {
		$this->mySQLi = $mySQLi;
		return $this;
	}

	/**
	 * get the last saved error occurred on: the lib, SQLite or Mysqli.
	 *
	 * @return Exception the error. or NULL of none
	 * @see SQLiteMySqli::$lastError
	 */
	public function getLastError() {
		return $this->lastError;
	}

	/**
	 * Export all the tables from MySQL to SQLite
	 *
	 * @return int the number of inserted rows on success. -1 on failure.
	 *        
	 * @see SQLiteMySqli::exportTableSQLiteMySqli()
	 */
	public function exportAllTablesSQLiteMySqli(): int {
		try {
			$this->beginTransactionSQLite();

			$tables = $this->getTablesFromMySQLi();
			$count = 0;

			foreach ($tables as $table) {
				$result = $this->exportTableSQLiteMySqli_( $table );
				$count += $result;
			}

			$this->commitTransactionSQLite();
			return $count;
		} catch (Exception $e) {

			$this->setError( $e );
			$this->rollbackTransactionSQLite();
		}

		return - 1;
	}

	/**
	 * Export a table from MySQL to SQLite
	 *
	 * @param string $mySQLiTableName
	 *        	the table to export from
	 * @param string $SQLiteTableName
	 *        	the destination. The table to import to. if empty, $mySQLiTableName will be used instead
	 *        	
	 * @return int the number of inserted rows on success. -1 on failure.
	 */
	public function exportTableSQLiteMySqli(String $mySQLiTableName, String $SQLiteTableName = ""): int {
		try {
			$this->checkNameOrThrowError( $mySQLiTableName, "mySQLiTableName" );

			$this->beginTransactionSQLite();
			$result = $this->exportTableSQLiteMySqli_( $mySQLiTableName, $SQLiteTableName );
			$this->commitTransactionSQLite();

			return $result;
		} catch (Exception $e) {

			$this->setError( $e );
			$this->rollbackTransactionSQLite();
		}

		return - 1;
	}

	/**
	 * Fetch the data from MySQL then insert them to SQLite
	 *
	 * @param string $mySQLiTableName
	 *        	the table to export from
	 * @param string $SQLiteTableName
	 *        	the table to import to. if NULL $mySQLiTableName will be used instead
	 * @param string $query
	 *        	the query to use to export the data from mySQLi
	 *        	
	 * @return int the number of inserted rows on success. -1 on failure.
	 */
	public function exportDataSQLiteMySqli(String $mySQLiTableName, String $SQLiteTableName = "", String $query): int {
		try {
			$this->checkNameOrThrowError( $mySQLiTableName, "mySQLiTableName" );

			$this->beginTransactionSQLite();
			$result = $this->sendFromMySQLiSQLiteMySqli( $mySQLiTableName, $SQLiteTableName, $query );
			$this->commitTransactionSQLite();

			return $result;
		} catch (Exception $e) {

			$this->setError( $e );
			$this->rollbackTransactionSQLite();
		}

		return - 1;
	}

	/**
	 *
	 * @return int the number of inserted rows on success.
	 *        
	 * @throws RuntimeException on faulure
	 * @see SQLiteMySqli::exportTableSQLiteMySqli
	 *
	 */
	protected function exportTableSQLiteMySqli_(String $mySQLiTableName, String $SQLiteTableName = null): int {
		if ($SQLiteTableName === null || strlen( $SQLiteTableName ) == 0) $SQLiteTableName = $mySQLiTableName;

		$columnsData = $this->getColumnsDataFromMySQLi( $mySQLiTableName );
		$this->createTableIfNotExistsSQLite( $columnsData, $SQLiteTableName );

		return $this->sendFromMySQLiSQLiteMySqli( $mySQLiTableName, $SQLiteTableName, "SELECT * FROM " . $mySQLiTableName );
	}


	/**
	 * Fetch the data from MySQL then insert them to SQLite
	 *
	 * @param string $mySQLiTableName
	 *        	the table to export from
	 * @param string $SQLiteTableName
	 *        	the table to import to
	 * @param string $query
	 *        	the query to use to export the data from mySQLi
	 *        	
	 * @return int the number of inserted rows on success.
	 *        
	 * @throws RuntimeException on failure.
	 */
	protected function sendFromMySQLiSQLiteMySqli(String $mySQLiTableName, String $SQLiteTableName = "", String $query): int {
		if ($SQLiteTableName === null || strlen( $SQLiteTableName ) == 0) $SQLiteTableName = $mySQLiTableName;

		$columnsSQLite = array_flip( $this->getColumnsFromSQLite( $SQLiteTableName ) );
		$columnsMySQL = array_flip( $this->getColumnsFromMySQLi( $mySQLiTableName ) );

		if ($this->ignorePrimaryKey) {
			$primaryKey = $this->getPrimaryColumnFromMySQLi( $mySQLiTableName );

			if ($primaryKey !== null) unset( $columnsMySQL[$primaryKey] );
		}

		foreach ($columnsMySQL as $column) {
			if (! isset( $columnsSQLite[$column] )) {
				unset( $columnsMySQL[$column] );
			}
		}

		$insertQuery = $this->generateInsertQuerySQLite( $SQLiteTableName, $columnsMySQL );
		$prepare = $this->getSQLite()->prepare( $insertQuery );

		if ($prepare === false) {
			throw new RuntimeException( $this->getSQLite()->lastErrorMsg() . ". Query: $insertQuery", $this->getSQLite()->lastErrorCode() );
		}

		$count = 0;


		$columnsMySQL = array_flip( $columnsMySQL );

		$results = $this->getMySQLi()->query( $query );
		if ($results === false) {
			throw new RuntimeException( $this->getMySQLi()->error . ". Query: $query", $this->getMySQLi()->errno );
		}

		while ($fetch = $results->fetch_assoc()) {
			foreach ($columnsMySQL as $column) {
				$prepare->bindParam( ":" . $column, $fetch[$column] );
			}

			if (! @$prepare->execute() && ! $this->ignoreErrors) {
				$results->free_result();
				throw new RuntimeException( $this->getMySQLi()->error, $this->getMySQLi()->errno );
			}
			else {
				$count ++;
			}

			$prepare->reset();
		}

		return $count;
	}

	/**
	 * get all the tables from the SQLite
	 *
	 * @return String[] array holds the tables name
	 *        
	 * @throws RuntimeException on query Failure
	 */
	protected function getTablesFromSQLite(): array {
		$query = "select name from SQLite_master where type = 'table'";
		$result = $this->SQLite->query( $query );

		if ($result === false) {
			throw new RuntimeException( $this->getSQLite()->lastErrorMsg(), $this->getSQLite()->lastErrorCode() );
		}

		$resultD = [];
		while ($fetch = $result->fetch_assoc()) {
			$resultD[] = $fetch;
		}

		$result->finalize();
		return $resultD;
	}

	/**
	 * get all the culomn's names of the table
	 *
	 * @param String $tableName
	 *        	the table to get its column's names
	 * @return String[] array holds all the column's names
	 *        
	 * @throws RuntimeException on query failure
	 */
	protected function getColumnsFromSQLite(String $tableName): array {
		$result = $this->getSQLite()->query( "SELECT * FROM " . $tableName . " WHERE 1=1 LIMIT 0" );
		if ($result === false) {
			throw new RuntimeException( $this->getSQLite()->lastErrorMsg(), $this->getSQLite()->lastErrorCode() );
		}

		$array = [];

		for($i = 0; $i < $result->numColumns(); $i ++) {
			$array[] = $result->columnName( $i );
		}

		$result->finalize();
		return $array;
	}

	/**
	 *
	 * @param String[] $array
	 *        	the data which are from MySQL "information_schema.COLUMNS"
	 * @param String $tableName
	 *        	Table's name
	 *        	
	 * @throws RuntimeException on query failure
	 */
	protected function createTableIfNotExistsSQLite(array $array, String $tableName) {
		$createTableQuery = "CREATE TABLE IF NOT EXISTS '" . $tableName . "' (";

		foreach ($array as $columnData) {
			$createTableQuery .= $this->convertColumnDatafromMySQLiSQLiteMySqliQuery( $columnData ) . ",";
		}

		$createTableQuery = substr( $createTableQuery, 0, strlen( $createTableQuery ) - 1 );
		$createTableQuery .= ");";

		if (! $this->getSQLite()->exec( $createTableQuery )) {
			throw new RuntimeException( $this->getSQLite()->lastErrorMsg() . ". Query: $createTableQuery", $this->getSQLite()->lastErrorCode() );
		}
	}

	/**
	 * Generate SQLite query for the prepare statement
	 *
	 * @param String $tableName
	 *        	the table to insert to
	 * @param String[] $columns
	 *        	columns name
	 *        	
	 * @return string the query to use to insert the data
	 */
	protected function generateInsertQuerySQLite(String $tableName, array $columns): string {
		$query = "INSERT " . ($this->ignoreErrors ? "OR IGNORE " : "");
		$query .= "INTO '" . $tableName . "' ";
		$parms = "(";
		$values = "VALUES (";

		foreach (array_keys( $columns ) as $key) {
			$parms .= $key . ", ";
			$values .= ":" . $key . ", ";
		}

		$parms = substr( $parms, 0, strlen( $parms ) - 2 );
		$values = substr( $values, 0, strlen( $values ) - 2 );

		return $query . $parms . ") " . $values . ");";
	}

	/**
	 * return the current selected database in MySqli
	 *
	 * @return String|NULL database's name if it's selected. NULL if none is selected.
	 *        
	 * @throws RuntimeException on query failure
	 */
	protected function getSelectedDatabase() {
		$query = "SELECT DATABASE();";

		$result = $this->getMySQLi()->query( $query );
		if ($result === FALSE) throw new RuntimeException( $this->getMySQLi()->error );

		return array_values( $result->fetch_assoc() )[0];
	}

	/**
	 * get all the tables from the MySQL
	 *
	 * @return String[] array holds the tables names
	 *        
	 * @throws RuntimeException on query failure
	 */
	protected function getTablesFromMySQLi(): array {
		$query = "SHOW TABLES";
		$DBResult = $this->getMySQLi()->query( $query );
		if ($DBResult === FALSE) throw new RuntimeException( $this->getMySQLi()->error );

		$fetchedArray = mySQLi_fetch_all( $DBResult );
		$DBResult->close();

		return array_column( $fetchedArray, 0 );
	}

	/**
	 * get all the culomns from a table
	 *
	 * @param String $tableName
	 * @return string[] array holds the columns
	 *        
	 * @throws RuntimeException on query failure
	 */
	protected function getColumnsFromMySQLi(String $tableName): array {
		$array = $this->getColumnsDataFromMySQLi( $tableName );

		$result = array();
		foreach ($array as $column) {
			$result[] = $column["COLUMN_NAME"];
		}

		return $result;
	}

	/**
	 * get <b>all</b> the culomns data of a table
	 * from "information_schema.COLUMNS"
	 *
	 * @param String $tableName
	 *        	the table to look up
	 * @param String $tableSchema
	 *        	TABLE_SCHEMA. To speed up the process and to avoid data directory scan
	 *        	
	 * @return String[] array holds all the columns data
	 *        
	 * @throws RuntimeException on query failure
	 * @see SQLiteMySqli::$databaseName
	 * @see SQLiteMySqli::$cachedColumnsSchema
	 */
	protected function getColumnsDataFromMySQLi(String $tableName): array {
		$tableSchema = ($this->databaseName !== null) ? $this->databaseName : $this->getSelectedDatabase();

		if ($tableSchema !== null && strlen( $tableSchema ) > 0) {
			$tableSchema = " AND TABLE_SCHEMA = '" . $tableSchema . "'";
		}
		else
			$tableSchema = "";

		if (isset( $this->cachedColumnsSchema[$tableName . "|" . $tableSchema] )) {
			return $this->cachedColumnsSchema[$tableName . "|" . $tableSchema];
		}

		$query = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = '" . $tableName . "'" . $tableSchema;
		$res = $this->getMySQLi()->query( $query );

		if ($res === false) {
			throw new RuntimeException( $this->getMySQLi()->error, $this->getMySQLi()->errno );
		}

		$results = $res->fetch_all( MYSQLI_ASSOC );

		// cache the results
		$this->cachedColumnsSchema[$tableName . "|" . $tableSchema] = $results;
		return $results;
	}

	/**
	 *
	 * @return String the column's name assoicated with the primary key. Or NULL if there is none
	 *        
	 * @throws RuntimeException on query failure
	 */
	protected function getPrimaryColumnFromMySQLi($tableName) {
		$query = "SHOW KEYS FROM $tableName WHERE Key_name = 'PRIMARY';";
		$res = $this->getMySQLi()->query( $query );
		if ($res === FALSE) throw new RuntimeException( $this->getMySQLi()->error );

		$array = $res->fetch_assoc();
		$res->free_result();

		if ($array === null) return null;
		return $array["Column_name"];
	}

	protected function setError($exception) {
		$this->lastError = $exception;
	}

	protected function beginTransactionSQLite(): bool {
		if ($this->transactionName === null || strlen( $this->transactionName ) === 0) return false;
		return $this->getSQLite()->exec( "SAVEPOINT " . $this->transactionName );
	}

	protected function commitTransactionSQLite(): bool {
		if ($this->transactionName === null || strlen( $this->transactionName ) === 0) return false;
		return $this->getSQLite()->exec( "RELEASE " . $this->transactionName );
	}

	protected function rollbackTransactionSQLite(): bool {
		if ($this->transactionName === null || strlen( $this->transactionName ) === 0) return false;
		return $this->getSQLite()->exec( "ROLLBACK TO " . $this->transactionName );
	}

	/**
	 * Convert a column from MySQLi to SQLite to use it and create table later.
	 * <p>
	 * <b>Right now it supports:</b>
	 * <i>AUTOINCREMENT, Integer Primary Key, default value, "NOT NULL" and COLUMN_TYPE</i>
	 *
	 * @param String[] $array
	 *        	the data which are from MySQL "information_schema.COLUMNS"
	 *        	
	 * @return string The line to create the column for SQLite in create table command
	 * @see #createTableIfNotExistsSQLite()
	 */
	protected static function convertColumnDatafromMySQLiSQLiteMySqliQuery(array $array): string {
		if ($array["EXTRA"] === "auto_increment") {
			$array["COLUMN_KEY"] = "PRI";
			$array["COLUMN_TYPE"] = "INTEGER";
			$autoIncrement = " AUTOINCREMENT";
		}
		else {
			$autoIncrement = "";
		}

		$columnName = "'" . $array["COLUMN_NAME"] . "'";

		$is_nullable = ($array["IS_NULLABLE"] === "YES") ? "" : " NOT NULL";
		$default = $array["COLUMN_DEFAULT"] === null ? "" : " DEFAULT '" . $array["COLUMN_DEFAULT"] . "'";
		$dataType = " " . $array["COLUMN_TYPE"];

		$primaryKey = $array["COLUMN_KEY"] === "PRI" ? " PRIMARY KEY" : "";

		return $columnName . $dataType . $primaryKey . $autoIncrement . $is_nullable . $default;
	}

	/**
	 * check the variable, whether it's safe to use in SQL queries.
	 *
	 * e.g. "SELECT * FROM ?" is not possible to use prepare on. check {@link mysqli::prepare()}
	 *
	 * @param String $variable
	 *        	the name to check
	 * @param String $variableName
	 *        	the variable's name to use, when throwing error about the variable
	 *        	
	 * @throws InvalidArgumentException if $variable does not meet all the specification
	 *        
	 * @see mysqli::prepare()
	 */
	protected static function checkNameOrThrowError($variable, String $variableName) {
		if ($variable == null) throw new InvalidArgumentException( "'$variableName' must not be NULL." );
		if (strlen( $variable ) == 0) throw new InvalidArgumentException( "'$variableName' must not be empty" );
		if (strlen( $variable ) > 32) throw new InvalidArgumentException( "'$variableName' must not be longer than 32 char" );

		if (! preg_match_all( "/^[a-zA-Z_]{1,32}[a-zA-Z0-9_]{0,32}$/", $variable )) throw new InvalidArgumentException( "'$variableName' must not start with number or contain anything than alphabits and numbers" );
	}
}