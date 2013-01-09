CSV LOADER SCRIPT

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

USAGE:
php -f load_csv_with_header.php 'test*.csv' table host dbanme username password

There is one TODO for you, as mentioned in the comments, if you need to deal with the situation of a 
system that occasionally forgot to give you headers (I did) you have to handle the default case.  

I hope this is helpful to you. Please let me know!

--Lonnie Webb, PMP
lonnie.webb@gmail.com

