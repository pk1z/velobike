Parser takes files from specified folder, sorts them by timestamp in the filename, starts processing from the most recent.
Each timestamp is checked in db for presence. If timestamp is already there, the whole process stops.
## Usage:
1. Configure your database and data source in parser_config.ini. Timestamp is expected to be between the first and the second underscores in the filename.
2. Run:
    ```
    $ python my_parser.py 
    ```