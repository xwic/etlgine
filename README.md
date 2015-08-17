# ETLgine
Java based tool to Extract, Transform and Load data from a source to a target

* Simple framework to configure 
    * One Extractor to load data from one or more sources
    * Any number of Transformers, to manipulate data during the process
    * One to many Loaders, that will load the data into a target (i.e. Table, Cube, File, …)
* Includes standard components that can be used, i.e.
    * Extractors from a JDBC Source (Database), (Zipped) CSV Files, Excel Files
    * Transformers for basic type conversion (Date, Boolean, Numbers, …)
    * Loaders to JDBC Target (Database), Cubes, CSV Files, Excel Files
* Define jobs in a Groovy Script to wire components and configure them
* ETLgine WebServer to schedule Jobs and monitor execution 
* Administration UI for Mappings and XWic Cube Management
