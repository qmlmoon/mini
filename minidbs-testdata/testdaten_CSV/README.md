INSTANCE SETUP GUIDE
====================

The MiniDBS Testinstance uses TPC-H data generated with a scaling factor of 0.02. To generate the TPC-H test data, follow these steps:

1. Generate TPC-H test data as CSV files
----------------------------------------

Go to your TPCH installation folder and type:

    $> dbgen -s 0.02

and then move the created CSV files to the Testinstance `testdaten_CSV` folder:

    $> mv *tbl /path/to/Testinstance/testdaten_CSV
    
2. Import the CSV data into the MiniDBS Testinstsance
-----------------------------------------------------

When the CSV files are available in the `testdaten_CSV` folder, you have to import them into the MiniDBS Testinstance by building the binary table and index files. To do this, you use the `ImportDriver` located in the `de.tuberlin.dima.minidb.test.qexec` package inside the MiniDBS_Test project. Additionally, you can also use the `StatisticsUpdater` from the same package to update the database statistics.