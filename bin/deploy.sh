#!/bin/bash

########################################################
# Declared Variables
########################################################

input=$1
sales_directory=/home/cloudera/salesdb
# or sales_directory=~/salesdb
hdfs_directory=/salesdb
path_to_files=/home/cloudera/zeroes_and_ones_Hadoop

# or path_to_files=~/csci5751_Hadoop

########################################################
# Functions
########################################################

display_help() {
    echo
    echo "Usage: sh ~/zeroes_and_ones_Hadoop/bin/deploy.sh -<option from below>"
    echo
    echo   -h, --help  :        display help contents
    echo   -g, --get_data  :         get sales data from url - Deliverable 2 step 1.1
    echo   -l, --load  :        load sales data to hdfs - Deliverable 2 step 1.2
    echo   -cr, --create_raw_db  :    create Sales raw database - Deliverable 2 step 1.3,1.4
    echo   -cs, --create_sales_db  :  create Sales database - Deliverable 2 step 2.2,2.3
    echo   -cv, --create_sales_views  :   create Sales views - Deliverable 2, step 2.4
    echo   -cps, --create_product_sales_partitioned  :     Create partitioned tables - Deliverable 3 step 1
    echo   -cpr, --create_product_region_sales_partitioned  :     Create partitioned tables - Deliverable 3 step 3
    echo   -cpv, --create_sales_partitioned_view  :     Create Sales view from partitioned table - Deliverable 3 step 2
    echo   -d, --drop :    drop all Views and DATABASES with CASCADE and delete all data from HDFS and disk - Deliverable 3, step 4
    exit 1
}

get_data() {
    echo Getting sales data from https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz
    sudo wget https://csci5751-2020sp.s3-us-west-2.amazonaws.com/sales-data/salesdata.tar.gz
    echo Unzipping sales data
    sudo tar -xvzf salesdata.tar.gz
    echo Deleting the zip file
    sudo rm -f salesdata.tar.gz
    echo Done!
}

load_hdfs() {
  echo Creating hdfs directory $hdfs_directory
  sudo -u hdfs hdfs dfs -mkdir $hdfs_directory

  for file in "$sales_directory"/*
     do
     echo processing $file
     filename=$(basename -- "$file")
     echo creating hdfs directory: $hdfs_directory/"${filename%.*}"
     sudo -u hdfs hdfs dfs -mkdir $hdfs_directory/"${filename%.*}"
     echo Adding file $sales_directory/$filename to hdfs directory: $hdfs_directory/"${filename%.*}"
     sudo -u hdfs hdfs dfs -put $sales_directory/$filename $hdfs_directory/"${filename%.*}"/
   done
   echo Changing owner of hdfs directory to hive
   sudo -u hdfs hdfs dfs -chown -R hive:hive $hdfs_directory
}

create_sales_raw_db() {
   impala-shell -f "$path_to_files"/sql/ddl_create_sales_raw.sql
}

create_sales_db() {
   impala-shell -f "$path_to_files"/sql/ddl_create_sales.sql
}

create_sales_views() {
   impala-shell -f "$path_to_files"/sql/create_sales_views.sql
}

create_product_sales_partitioned() {
   impala-shell -f "$path_to_files"/sql/ddl_create_product_sales_partitioned.sql
}

create_product_region_sales_partitioned() {
   impala-shell -f "$path_to_files"/sql/ddl_create_product_region_sales_partitioned.sql
}

create_sales_partitioned_view() {
   impala-shell -f "$path_to_files"/sql/create_sales_partitioned_view.sql
} 


drop_raw_db() {
   impala-shell -q "DROP DATABASE IF EXISTS zeroes_and_ones_sales_raw CASCADE;"
}

drop_sales_db() {
   impala-shell -q "DROP TABLE IF EXISTS zeroes_and_ones_sales.customers PURGE;"
   impala-shell -q "DROP TABLE IF EXISTS zeroes_and_ones_sales.products PURGE;"
   impala-shell -q "DROP TABLE IF EXISTS zeroes_and_ones_sales.sales PURGE;"
   impala-shell -q "DROP TABLE IF EXISTS zeroes_and_ones_sales.employees PURGE;"
   impala-shell -q "DROP TABLE IF EXISTS zeroes_and_ones_sales.product_sales_partition PURGE;"
   impala-shell -q "DROP TABLE IF EXISTS zeroes_and_ones_sales.product_region_sales_partition PURGE;"
   impala-shell -q "DROP DATABASE IF EXISTS zeroes_and_ones_sales CASCADE;"
}

drop_sales_views() {
    impala-shell -q "Drop VIEW IF EXISTS zeroes_and_ones_sales.customer_monthly_sales_2019_view;"
    impala-shell -q "Drop VIEW IF EXISTS zeroes_and_ones_sales.top_ten_customers_amount_view;"
    impala-shell -q "Drop VIEW IF EXISTS zeroes_and_ones_sales.customer_monthly_sales_2019_partitioned_view;"
}

delete_hdfs_raw() {
    sudo -u hdfs hdfs dfs -rm -r $hdfs_directory
}

delete_raw_data() {
    sudo rm -rf salesdb
}

########################################################
# Run Time Commands
########################################################

for i in {1}
 do
    case $input in
      -h | --help)
          display_help
          ;;

      -g | --get_data)
          echo "Geting data and unzipping file"
          get_data
          ;;

      -l | --load)
          echo "Loading data to HDFS"
          load_hdfs
          ;;

      -cr | --create_raw_tables)
          echo "Creating raw external tables"
          create_sales_raw_db
          ;;

      -cs | --create_sales_tables)
          echo "Creating Sales managed parquet tables as Select on Sales raw DATABASE"
          create_sales_db
          ;;

      -cv | --create_sales_views)
          echo "Creating Sales Views"
          create_sales_views
          ;;

      -cps | --create_product_sales_partitioned)
          echo "Creating Product and Sales partitioned table"
          create_product_sales_partitioned
          ;;

      -cpr | --create_product_region_sales_partitioned)
          echo "Creating Product Region and Sales partitioned table"
          create_product_region_sales_partitioned
          ;;

      -cpv | --create_sales_partitioned_view)
          echo "Creating partitioned sales view"
          create_sales_partitioned_view
          ;;


      -d | --drop)
          echo "Dropping all Views and DATABASES with CASCADE and deleting all data from HDFS and disk"
          echo "Dropping Sales Views"
          drop_sales_views
          echo "Dropping Sales DATABASE CASCADE"
          drop_sales_db
          echo "Dropping Sales raw DATABASE CASCADE"
          drop_raw_db
          echo "Removing data from HDFS"
          delete_hdfs_raw
          echo "Removing downloaded data from /Home/Cloudera/"
          delete_raw_data

          ;;

      *)  #Default option
          echo "Error: Unknown input: $1"
          echo "Use \"sh ~/zeroes_and_ones_Hadoop/bin/deploy.sh -h\" to display input options"
          exit 1
          ;;

    esac
done