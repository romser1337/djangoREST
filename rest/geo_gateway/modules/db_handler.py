#Version 0.15
#Change log:
#30.01.2024 Execute function adjustent to eliminate 'idle in transaction' issues in postgres
#15.02.2024 df_to_sql method added for dataframes uploading to SQL

import psycopg2
from psycopg2 import OperationalError
import traceback
import time
import pandas as pd
from sqlalchemy.orm import sessionmaker  # for df_to_sql method
from sqlalchemy import create_engine  # for df_to_sql method

class DBHandler:

    def __init__(self, host="localhost", port=5432, user="postgres", password="smacap", dbname=""):
        # Storing the parameters as instance attributes
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
        except OperationalError as e:
            if "does not exist" in str(e):
                # If database doesn't exist, connect to default DB and create the new one
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname="postgres"  # default database
                )
                self.connection.autocommit = True  # Required for executing CREATE DATABASE
                with self.connection.cursor() as cursor:
                    cursor.execute(f"CREATE DATABASE {self.dbname};")
                self.connection.close()

                # Reconnect to the new database
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.dbname
                )
            else:
                raise e

        self.cursor = self.connection.cursor()
        
    def fetch(self, query):
        """Predefined fetch operation."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
                print(f"Error: {e}")
                db_engine.rollback()
    
    def execute(self, query, column_names=False, params=None):
        """Custom query execution with error handling, commit and rollback."""
        try:
            with self.connection.cursor() as cursor:
                # Check if params is a list of tuples for bulk insert
                if params and isinstance(params, list) and all(isinstance(p, tuple) for p in params):
                    cursor.executemany(query, params)
                elif params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
    
                # If it's a SELECT query, return results
                if query.strip().lower().startswith("select"):
                    ### [CHANGE]: Fetch column names if the query is "SELECT *"
                    if query.strip().lower().startswith("select *") and column_names is True:
                        column_names = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        return column_names, rows
                    else:
                        return cursor.fetchall()
    
        except Exception as e:
            self.connection.rollback()
            error_message = traceback.format_exc()
            print(f"An error occurred: {e}\n{error_message}")
            raise e

        else:
            self.connection.commit()  # Only commit if there's no error

        finally:
            if query.strip().lower().startswith("select"):
                self.connection.rollback()  # Close transaction for SELECT queries


    def df_to_sql(self, dataframe, table_name, if_exists='replace', index=False, chunksize=None):
        """
        Uploads a DataFrame to the specified SQL table using the to_sql method.

        Parameters:
        - dataframe (pd.DataFrame): The DataFrame to upload.
        - table_name (str): The name of the target SQL table.
        - if_exists (str): What to do if the table already exists.
        - index (bool): Whether to write the DataFrame's index as a column.
        - chunksize (int or None): Specifies the number of rows in each batch to be written at a time.
        """
        try:
            # SQLAlchemy engine
            engine_url = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}'
            engine = create_engine(engine_url)
            Session = sessionmaker(bind=engine)
            session = Session()

            # Begin transaction
            with engine.begin() as connection:
                # Upload DataFrame to SQL within a transaction
                dataframe.to_sql(table_name, connection, if_exists=if_exists, index=index, chunksize=chunksize, method='multi')
            
            # Commit transaction and close session
            session.commit()
            session.close()
            print(f'DataFrame uploaded to table {table_name} successfully.')

        except Exception as e:
            session.rollback()  # Rollback in case of error
            session.close()
            error_message = traceback.format_exc()
            print(f"An error occurred: {e}\n{error_message}")
    
    
    def drop_table(self, table_name):
        """
        Drop a table from the database.

        Parameters:
        - table_name (str): Name of the table to drop.
        """
        try:
            query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            self.execute(query)
            self.connection.commit()
            print(f"Table {table_name} dropped successfully!")
        except Exception as e:
            print(f"Error dropping table {table_name}: {e}")
            self.rollback()
    
    def reconnect(self):
        """Reconnect to the database."""
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname
            )
        self.cursor = self.connection.cursor()
        
    def close(self):
        """Close the cursor and the connection."""
        self.cursor.close()
        self.connection.close()

    def drop_database(self, dbname):
        """Drop the specified database after terminating its connections."""
        self.close()
        self.reconnect()
        self.connection.autocommit = True

        # Terminate all connections to the specified database
        self.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{dbname}';")

        # Drop the specified database
        self.execute(f'DROP DATABASE {dbname};')

        self.connection.autocommit = False  # Revert back to transaction mode

    def reset_transaction(self):
        """Reset the transaction to a clean state."""
        self.connection.rollback()

    def rollback(self):
        """Reset the transaction to a clean state."""
        self.connection.rollback()

    def commit(self):
        """Commit the transaction."""
        self.connection.commit()

    def columns(self, table_name):
        try:
            # Fetch column details
            col_details = self.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table_name}'")
            
            # Print column details
            print(table_name, " Column Details:")
            print("-" * 40)
            for col_name, data_type in col_details:
                print(f"{col_name.ljust(20)} | {data_type}")
            print("-" * 40)
            
            # Fetch sample rows
            sample_rows = self.execute(f"SELECT * FROM {table_name} LIMIT 5")
            
            # Print sample rows
            print("\nSample Rows:")
            print("-" * 40)
            for row in sample_rows:
                print(" | ".join(map(str, row)))
            print("-" * 40)
    
        except Exception as e:
            print(f"Error fetching table details: {e}")
            self.rollback()
            raise e

    def flush(self, db_name):
        """
        Terminates all connections to the specified database and drops the database.
        """
        # Ensure auto commit is on
        self.connection.autocommit = True
        
        # Terminate all connections to the specified database
        print(f"Attempting to terminate all connections to the '{db_name}' database...")
        try:
            self.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}';")
            print(f"All connections to the '{db_name}' database terminated successfully!")
        except Exception as e:
            print(f"Error terminating connections: {e}")
            return

        # Drop the database
        print(f"Attempting to drop the '{db_name}' database...")
        try:
            self.execute(f'DROP DATABASE {db_name};')
            print(f"Database '{db_name}' dropped successfully!")
        except Exception as e:
            print(f"Error dropping database: {e}")
        finally:
            # Revert back to transaction mode if desired
            self.connection.autocommit = False
            print(f"Connection to '{db_name}' set back to transaction mode.")
        self.close()
        print(f"Connection to '{db_name}' is closed.")

