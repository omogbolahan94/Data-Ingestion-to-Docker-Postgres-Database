## ABOUT
Migrating a source CSV data to a postgres database

## DATA SOURCE
* The original data was download from Kaggle: [kaggle](https://www.kaggle.com/datasets/anandaramg/taxi-trip-data-nyc)
* To avoid downloading the data in the application over the internet, I used Python's **http.server** module to serve the source data directory on the local machine's IP address. To achieve this, follow these steps:
    * Start a local server in the project directory in terminal by running:
    ```{py}
       python -m http.server
    ```
    * Check your local machine's IP address in a new terminal:
    ```{py}
       ipconfig
    ```
    * Access the project directory in a web browser using the URL:
    ``` {py}
        http://<your_ip_address>/path_to_data_directory
    ```

## Objective
* Created a docker network
* Built application image: Containerize a python script that ingested csv data from source to postges 
* Created a pgAdmin container
* Created a postgres container 
* Used a common network to run the application image, pgAdmin and postgres container.
* Run the application image with the appropriate parameters to migrate the source data to postgres database and then use pgAdmin to interact with the database.