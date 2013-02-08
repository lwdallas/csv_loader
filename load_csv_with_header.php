<?php


/*

	This script will import any CSV altho you may want to add data type handling.
	It handles filemasks in place of filename, too.  NOTE: Files do not need to be identical in layout.
	This version uses PostgreSQL.  Change the pg_ functions for your abstraction library or mysql_.
	The magic is that your headers will define your file layout.
	If your files change order from file to file, don't worry, new columns will be added, widths changed.
	Pretty much everything is treated as varchar.

	For your table you have to do at least this much:
	create table tablename ()

	Columns in the CSV may not be named file_date.
	For my application I needed to load several files with date in their name and used that in the record.
	Just drop that column afterward if you don't need it.

	Version 0.01
	Inital Release

	Version 0.02
	Auto pipe handling, because people suck and occasionally use pipes.

 */

$error_count=0;
$seperator=',';
$auto_use_pipes=false;

if (sizeof( $argv )<= 1){
	die("\nUSAGE:\n load_csv_with_header file_name table_name host dbname user password [--auto-use-pipes] \n\nLoad CSV files into an existing table.  It doesn't have to have columns initially.\nModify the TODO line in the source if you haven't already.\nOptional auto-use-pipes feature detects pipes in the header and changes the seperator to pipes as needed.\n");
}

main();

echo "\n---\nErrors=$error_count\n---\n";

function main(){

	$filename='';
	global $argv, $seperator, $auto_use_pipes;

	// open pg connection
	echo 'open pg connection: host='.$argv[3].' dbname='.$argv[4].' user='.$argv[5].' password='.$argv[6]."\n";
	$dbh = pg_connect('host='.$argv[3].' dbname='.$argv[4].' user='.$argv[5].' password='.$argv[6]);
	if (!$dbh) {
	    die("Error in connection: " . pg_last_error());
	}

	truncate_table( $dbh, $argv[2] );
	try{
		add_date_column( $dbh, $argv[2], 'file_date' );
	} catch(Exception $e){
		// do nothing, this exception is allowed
	}

	// close connection
	pg_close($dbh);

	foreach ($argv as $arg) {
		if ('--auto-use-pipes'==$arg){
			$auto_use_pipes=true;
			echo "AUTO_USE_PIPES=TRUE\n";
			break;
		}
	}

	foreach (glob( $argv[1] ) as $filename){
		import_file($filename);

	}
}

function add_column( $dbh, $table_name, $field_name, $field_size ){
	$add_sql='ALTER TABLE '.$table_name.' ADD COLUMN '.$field_name.' VARCHAR('.$field_size.') NULL;';
	try{
	pg_query( $dbh, $add_sql );
	} catch(Exception $e){
		// do nothing, this exception is allowed
	}

}

function add_date_column( $dbh, $table_name, $field_name ){
	$add_sql='ALTER TABLE '.$table_name.' ADD COLUMN '.$field_name.' DATE;';
	try{
	pg_query( $dbh, $add_sql );
	} catch(Exception $e){
		// do nothing, this exception is allowed
	}

}

function truncate_table( $dbh, $table_name ){
	$truncate_sql='TRUNCATE TABLE '.$table_name.' RESTART IDENTITY;';
	try{
		pg_query( $dbh, $truncate_sql );
	} catch(Exception $e){
		// do nothing, this exception is allowed
	}
}

function import_file($filename){
	global $argv, $seperator, $auto_use_pipes;

	echo "\nimporting $filename\n";

	// open pg connection
	$dbh = pg_connect('host='.$argv[3].' dbname='.$argv[4].' user='.$argv[5].' password='.$argv[6]);
	if (!$dbh) {
	    die("Error in connection: " . pg_last_error());
	}

	$result=null;

	// open the text file
	$fp = fopen ($filename, "r");
	$filename = basename( $filename );

	// initialize a loop to go through each line of the file
	// create array for columns
	if ($columns = fgetcsv($fp,1024, $seperator)) {

		// check for pipes
		if ($auto_use_pipes && stripos( $columns[0], '|' )){
			$seperator = '|';
			echo "Seperator is now: ".$seperator."\n";
			$columns=explode('|', $columns[0]);
		}

	echo "\nheader:\n".implode( $seperator, $columns);

		// get the date for the file
		// assuming in the format cards_YYYYMMDD_xxxx.csv
		$date_part = substr($filename, strpos($filename, '_')+1, 8);
		$file_date = substr( $date_part, 0, 4).'-'.substr( $date_part, 4, 2).'-'.substr( $date_part, 6, 2);

		/* NOTE: if you don't use filedates in your naming convention remove the file_date from the insert below */

		// if the first row has no headers, use the default format
		if ($columns[0]=='ID'){
			/* TODO
			put a default table structure in here ESPECIALLY if you don't have headers in all your files
			you also need a better sanity check than 'id'--it is an example

			This criteria occured for me when I had a datafile that had no headers but followed the default format.
			I don't know why but my data was always the same in the first column if there was no data.  Go figure.
			*/

			//$sql='insert into '.$argv[2].' ( id, name, pet, file_date) VALUES ( $1, $2, $3,'."'$file_date'".')';
			$sql='insert into '.$argv[2].' ( id, name, pet, file_date) VALUES ( $1, $2, $3,'."'$file_date'".')';
			echo "\nsql=>".$sql;
		} else {
			// else load the headers and dynamically build the prepared stmt and add any missing columns
			echo "\nreading columns...";

			// go ahead and create sql stmt
			$sql = "insert into ".$argv[2]." (";
			$comma = ' ';
		    for ($i = 0, $j = count($columns); $i < $j; $i++) {
		        $sql .= $comma.$columns[$i];
		        $comma=', ';
		    }
	        $sql .= $comma.file_date;
			$sql .= ") VALUES (";
			$comma = ' ';
		    for ($i = 0, $j = count($columns); $i < $j; $i++) {
		        $sql .= $comma.'$'.(1+$i);
		        $comma=', ';
		    }
		    $sql .= $comma."'$file_date'";
			$sql .= ")";

			echo "\nchecking columns";
			$meta = pg_meta_data($dbh, $argv[2]);
			if (is_array($meta)) {
				for ($i = 0, $j = count($columns); $i < $j; $i++) {
					//var_dump($meta);
					if (array_key_exists(strtolower($columns[$i]),$meta)) {
						echo '.';
					} else {
						echo "\nadding column ".$columns[$i];
						add_column( $dbh, $argv[2], $columns[$i], 35);
					}
				}
			}
		}
		echo "\n prepared stmt: ".$sql;
		$stmt = pg_prepare($dbh, "ps", $sql);
	}

	global $error_count;

	$count=0;
	while($csv_line = fgetcsv($fp,1024, $seperator)) {

echo "\nline:\n".implode( $seperator, $csv_line );

		$fields=array();
		$count++;
	    for ($i = 0, $j = count($csv_line); $i < $j; $i++) {
			$fields[$i]=pg_escape_string( substr($csv_line[$i],0, 35));
	    }
		$result = pg_execute($dbh, "ps", $fields);
		if (!$result) {
			$error_count++;
			echo "\n---\nError in SQL insert $count: " . pg_last_error()."\n---\n";
		}
	}

	fclose ($fp);

	// free memory
	pg_free_result($result);

	// close connection
	pg_close($dbh);
}

?>