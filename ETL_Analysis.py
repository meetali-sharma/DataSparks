import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from ydata_profiling import ProfileReport
from datetime import timedelta


# Reading CSV files
customer_data = pd.read_csv('Customers.csv', encoding='latin-1')
exchange_data = pd.read_csv('Exchange_Rates.csv', encoding='latin-1')
stores_data = pd.read_csv('Stores.csv',encoding='latin-1')
sales_data = pd.read_csv('Sales.csv',encoding='latin-1')
product_data = pd.read_csv('Products.csv', encoding='latin-1')

#Creating a data profile for Customer
profile = ProfileReport(customer_data, title="Customer Profiling Report")
profile.to_file('Customer_Analysis.html')
print("Report Created")

#Creating a data profile for Exchange Rate
exchange_profile = ProfileReport(exchange_data, title="Exchange Rate Profiling Report")
exchange_profile.to_file('Exchange_Analysis.html')
print("Report Created")


#Creating a data profile for Stores
stores_profile = ProfileReport(stores_data, title="Store Profiling Report")
stores_profile.to_file('Stores_Analysis.html')
print("Report Created")


#Creating a data profile for Sales
sales_profile = ProfileReport(sales_data, title="Sales Profiling Report")
sales_profile.to_file('Sales_Analysis.html')
print("Report Created")


#Creating a data profile for Product
product_profile = ProfileReport(product_data, title="Product Profiling Report")
product_profile.to_file('Product_Analysis.html')
print("Report Created")


""" This procedure is for Customers """
#Checking missing records in the columns
missing_rows = customer_data[customer_data.isnull().any(axis=1)]
print(missing_rows)


#Replacing the missing values
customer_data.fillna('NA', inplace=True)
print(customer_data)

#Printing Result after filling the missing values
missing_rows = customer_data[customer_data.isnull().any(axis=1)]
print(missing_rows)

""" This procedure is for Exchange rates """
#Checking missing records in the columns
exchange_missing_rows = exchange_data[exchange_data.isnull().any(axis=1)]
print(exchange_missing_rows)

""" This procedure is for Products """
#Checking missing records in the columns
product_missing_rows = product_data[product_data.isnull().any(axis=1)]
print(product_missing_rows)

""" This procedure is for Sales """
#Check for missing records
sales_missing_rows =sales_data[sales_data.isnull().any(axis=1)]
print(sales_missing_rows)

# Convert to datetime
sales_data['Order Date'] = pd.to_datetime(sales_data['Order Date'], format='%m/%d/%Y')
sales_data['Delivery Date'] = pd.to_datetime(sales_data['Delivery Date'], format='%m/%d/%Y')

# Calculate the difference while the columns are in datetime format
difference = sales_data['Delivery Date'] - sales_data['Order Date']
sales_data['Delivery Date'] = sales_data['Delivery Date'].fillna(sales_data['Order Date'] + timedelta(days=7))

# Now, if you need the dates in a specific string format for display, you can format them:
sales_data['Order Date'] = sales_data['Order Date'].dt.strftime('%m-%d-%Y')
sales_data['Delivery Date'] = sales_data['Delivery Date'].dt.strftime('%m-%d-%Y')

sales_missing_rows =sales_data[sales_data.isnull().any(axis=1)]
print(sales_missing_rows)

""" This procedure is for Store """
#Check for missing records
store_missing_rows = stores_data[stores_data.isnull().any(axis=1)]
print(store_missing_rows)

#Finding mean value to replace the missing data
mean_value=stores_data['Square Meters'].mean()
stores_data['Square Meters']=stores_data['Square Meters'].fillna(mean_value)

missing_rows =stores_data[stores_data.isnull().any(axis=1)]
print(missing_rows)

#Establishing connection with MSSQL
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-NCF2KBL;"
    "DATABASE=DataSparks;"
    "UID=Meetali;"
    "PWD=Welcome_123;"
)

# Establish the connection
try:
    # Connect to the SQL Server database
    connection = pyodbc.connect(connection_string)

    # Create a cursor from the connection
    cursor = connection.cursor()

    # Creating Customer table
    cursor.execute("""
            DROP TABLE IF EXISTS [dbo].[Customers];
            CREATE TABLE [dbo].[Customers](
                [CustomerKey] [int] NOT NULL,
    	        [Gender] [nvarchar](50) NOT NULL,
    	        [Name] [nvarchar](50) NOT NULL,
    	        [City] [nvarchar](50) NOT NULL,
    	        [State_Code] [nvarchar](50) NOT NULL,
    	        [State] [nvarchar](50) NOT NULL,
    	        [Zip_Code] [nvarchar](50) NOT NULL,
    	        [Country] [nvarchar](50) NOT NULL,
    	        [Continent] [nvarchar](50) NOT NULL,
    	        [Birthday] [date] NOT NULL,
            CONSTRAINT [PK_Customers] PRIMARY KEY CLUSTERED
            (
                [CustomerKey] ASC
            ));
        """)

    # Commit the transaction
    connection.commit()
    print("Customers table created successfully!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    # Inserting the data from dataframe into the table
    insert_query = """
    INSERT INTO Customers (CustomerKey, Gender, Name, City, State_Code, State, Zip_code, Country, Continent, Birthday)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Prepare data for insertion (extracting specific columns from the DataFrame)
    data_to_insert = [tuple(row) for row in customer_data[['CustomerKey', 'Gender', 'Name', 'City', 'State Code', 'State', 'Zip Code', 'Country', 'Continent', 'Birthday']].itertuples(index=False, name=None)]

    # Execute the batch insert into specific columns
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    connection.commit()

    print("Data inserted successfully into Customers table!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    #Creating Exchange Rates table
    cursor.execute("""
                DROP TABLE IF EXISTS [dbo].[Exchange_Rates];
                CREATE TABLE [dbo].[Exchange_Rates](
    	        [Date] [date] NOT NULL,
    	        [Currency] [nvarchar](50) NOT NULL,
    	        [Exchange] [float] NOT NULL,
                CONSTRAINT [PK_Exchange_Rates] PRIMARY KEY CLUSTERED 
            (
            	[Date] ASC,
    	        [Currency] ASC
            ));
            """)

    # Commit the transaction
    connection.commit()

    print("Exchange Rates table created successfully!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    # Inserting the data from dataframe into the table
    insert_query = """
        INSERT INTO Exchange_Rates (Date, Currency, Exchange)
        VALUES (?, ?, ?)
        """

    # Prepare data for insertion (extracting specific columns from the DataFrame)
    data_to_insert = [tuple(row) for row in
                      exchange_data[['Date', 'Currency', 'Exchange']].itertuples(index=False, name=None)]

    # Execute the batch insert into specific columns
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    connection.commit()

    print("Data inserted successfully into Exchange Rates table!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    #Creating Stores
    cursor.execute("""
            DROP TABLE IF EXISTS [dbo].[Stores];
            CREATE TABLE [dbo].[Stores](
	        [StoreKey] [int] NOT NULL,
	        [Country] [nvarchar](50) NOT NULL,
	        [State] [nvarchar](50) NOT NULL,
	        [Square_Meters] [smallint] NULL,
	        [Open_Date] [date] NOT NULL,
        CONSTRAINT [PK_Stores] PRIMARY KEY CLUSTERED 
    (
        [StoreKey] ASC

    ));
        """)

    # Commit the transaction
    connection.commit()

    print("Stores table created successfully!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    #Inserting data from data frame into table
    insert_query = """
    INSERT INTO Stores(StoreKey, Country, State, Square_Meters, Open_Date)
    VALUES (?, ?, ?, ?, ?)
    """

    # Prepare data for insertion (extracting specific columns from the DataFrame)
    data_to_insert = [tuple(row) for row in stores_data[['StoreKey', 'Country', 'State', 'Square Meters', 'Open Date']].itertuples(index=False, name=None)]

    # Execute the batch insert into specific columns
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    connection.commit()

    print("Data inserted successfully into Stores table!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    #Creating Sales table
    cursor.execute("""
                DROP TABLE IF EXISTS [dbo].[Sales];
                CREATE TABLE [dbo].[Sales](
    	        [Order_Number] [int] NOT NULL,
    	        [Line_Item] [tinyint] NOT NULL,
    	        [Order_Date] [date] NOT NULL,
    	        [Delivery_Date] [date] NULL,
    	        [CustomerKey] [int] NOT NULL,
    	        [StoreKey] [int] NOT NULL,
    	        [ProductKey] [int] NOT NULL,
    	        [Quantity] [tinyint] NOT NULL,
    	        [Currency_Code] [nvarchar](50) NOT NULL,
                CONSTRAINT [PK_Sales] PRIMARY KEY CLUSTERED 
        (
    	    [Order_Number] ASC,
    	    [Line_Item] ASC
        ));
            """)

    # Commit the transaction
    connection.commit()

    print("Sales table created successfully!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    # Inserting data from data frame into table
    insert_query = """
        INSERT INTO Sales (Order_Number, Line_Item, Order_Date, Delivery_Date, CustomerKey, StoreKey, ProductKey, Quantity, Currency_Code )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    # Prepare data for insertion (extracting specific columns from the DataFrame)
    data_to_insert = [tuple(row) for row in sales_data[
        ['Order Number', 'Line Item', 'Order Date', 'Delivery Date', 'CustomerKey', 'StoreKey', 'ProductKey',
         'Quantity', 'Currency Code']].itertuples(index=False, name=None)]

    # Execute the batch insert into specific columns
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    connection.commit()

    print("Data inserted successfully into Sales table!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    #Creating Product table
    cursor.execute("""
                DROP TABLE IF EXISTS [dbo].[Products];
               CREATE TABLE [dbo].[Products](
    	        [ProductKey] [int] NOT NULL,
    	        [Product_Name] [nvarchar](100) NOT NULL,
    	        [Brand] [nvarchar](50) NOT NULL,
    	        [Color] [nvarchar](50) NOT NULL,
    	        [Unit_Cost_USD] [nvarchar](50) NOT NULL,
    	        [Unit_Price_USD] [nvarchar](50) NOT NULL,
    	        [SubcategoryKey] [int] NOT NULL,
    	        [Subcategory] [nvarchar](50) NOT NULL,
    	        [CategoryKey] [int] NOT NULL,
    	        [Category] [nvarchar](50) NOT NULL,
            CONSTRAINT [PK_Products] PRIMARY KEY CLUSTERED 
        (
    	    [ProductKey] ASC
        ));
            """)

    # Commit the transaction
    connection.commit()

    print("Products table created successfully!")

except pyodbc.Error as e:
    print("Error in connection or execution:", e)

try:
    insert_query = """
        INSERT INTO Products (ProductKey, Product_Name, Brand, Color, Unit_Cost_USD, Unit_Price_USD, SubcategoryKey, Subcategory, CategoryKey, Category)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    # Prepare data for insertion (extracting specific columns from the DataFrame)
    data_to_insert = [tuple(row) for row in product_data[
        ['ProductKey', 'Product Name', 'Brand', 'Color', 'Unit Cost USD', 'Unit Price USD', 'SubcategoryKey',
         'Subcategory', 'CategoryKey', 'Category']].itertuples(index=False, name=None)]

    # Execute the batch insert into specific columns
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    connection.commit()

    print("Data inserted successfully into Products table!")

    # Close the cursor and connection
    cursor.close()
    connection.close()

except pyodbc.Error as e:
    print("Error in connection or execution:", e)