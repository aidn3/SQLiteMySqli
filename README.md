# SQLiteMySqli
Class designed to ease the exportion and importion from and to SQLite and MySqli  
  
## Usage Example:  

```PHP
//include the Lib in Project
require "DB_ME.php";

$SQLiteMySqli = new SQLiteMySqli();
$SQLiteMySqli->setMySQLi( getMySqliInstance() );
$SQLiteMySqli->setSQLite( "filePathSQLiteMySqli.db" );

$result = $SQLiteMySqli->exportTableSQLiteMySqli( "students" );

if ($result >= 0) echo "$result exported rows";
else throw $SQLiteMySqli->getLastError();
```


