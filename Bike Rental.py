import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import glob
import sqlite3
from sqlalchemy import create_engine, ForeignKey, Column, String, Integer, CHAR, DATE, Integer, TIME
from sqlalchemy.orm import sessionmaker, declarative_base

#--- Part 1: Citi Bike Data ---#
#Import Citi Bike Data
folder_path = "C:\\Users\\garre\\OneDrive\\Desktop\\Port_Project\\Bike Rental Management\\Bike Citi"
citi_excel_files = glob.glob(f"{folder_path}/*.csv")
dataframes = [pd.read_csv(file) for file in citi_excel_files]
bike_df = pd.concat(dataframes, ignore_index=True)

#Inspection of Citi Bike Data
#print(bike_df.describe())
#print(bike_df.info())
#print(bike_df.isna().sum())
#print(bike_df.duplicated().sum())

#Inspect Missing Values
if 'Start Time' in bike_df.columns and 'Birth Year' in bike_df.columns and 'User Type' in bike_df.columns:
    bike_df['Start Time'] = pd.to_datetime(bike_df['Start Time'])
    bike_df['Birth Year'] = pd.to_numeric(bike_df['Birth Year'])
    bike_df.set_index('Start Time', inplace=True)

    monthly_birthyear_missing = bike_df['Birth Year'].isnull().resample('ME').sum()
    #print('birth year breakdown:\n', monthly_birthyear_missing)

    monthly_usertype_missing = bike_df['User Type'].isnull().resample('ME').sum()
    #print('User Type breakdown:\n', monthly_usertype_missing)

    bike_df.reset_index(inplace=True)

#Fill Empty Data Values with 'N/A'
bike_df = bike_df.fillna(value='N/A')
#Fix Gender Column -> 0 = N/A
bike_df['Gender'] = bike_df['Gender'].replace(0, 'N/A')


#Seperate Date and Time
bike_df['Start Time'] = pd.to_datetime(bike_df['Start Time'])
bike_df['Stop Time'] = pd.to_datetime(bike_df['Stop Time'])
bike_df['Date'] = bike_df['Start Time'].dt.date
bike_df['Start Time'] = bike_df['Start Time'].dt.time
bike_df['Stop Time'] = bike_df['Stop Time'].dt.time
#Add an Index/reorder columns
bike_df['ID'] = bike_df.index
col_combined_id = bike_df.pop('ID')
bike_df.insert(0, "ID", col_combined_id)
date_id = bike_df.pop('Date')
bike_df.insert(1, "Date", date_id)
duration_id = bike_df.pop('Trip Duration')
bike_df.insert(4, 'Trip Duration', duration_id)

#Rename Columns
bike_df.rename(columns={'Start Time': 'Start_Time',
                        'Trip Duration': 'Trip_Duration',
                        'Stop Time': 'Stop_Time',
                        'Start Station ID': 'Start_Station_ID',
                        'Start Station Name': 'Start_Station_Name',
                        'End Station ID': 'End_Station_ID',
                        'End Station Name': 'End_Station_Name',
                        'Bike ID': 'Bike_ID',
                        'User Type': 'User_Type',
                        'Birth Year': 'Birth_Year',
                        'Start Station Latitude': 'Start_Station_Latitude',
                        'Start Station Longitude': 'Start_Station_Longitude',
                        'End Station Latitude': 'End_Station_Latitude',
                        'End Station Longitude': 'End_Station_Longitude'}, inplace=True)

#Check Final Results
#print(bike_df.describe())
#print(bike_df.isna().sum())
#print(bike_df.head())
#print(bike_df.info())

#--- Part 2: Weather Data ---#
# Import Weather Data
weather_data_path = "C:\\Users\\garre\\OneDrive\\Desktop\\Port_Project\\Bike Rental Management\\Weather\\newark_airport_2016.csv"
weather_df = pd.read_csv(weather_data_path)

#Inspect the Data
#print(weather_df.head())
#print(weather_df.describe())
#print(weather_df.info())
#print(weather_df.isna().sum())

#Clean Up Columns
weather_df = weather_df.drop(['PGTM', 'TSUN', 'STATION', 'NAME'], axis=1)
weather_df.rename(columns={'AWND': 'Avg_Wind_Speed',
                            'PRCP': 'Precipitation',
                            'SNOW': 'Snowfall',
                            'SNWD': 'Snow_Depth',
                            'TAVG': 'Avg_Temp',
                            'TMAX': 'Max_Temp',
                            'TMIN': 'Min_Temperature',
                            'WDF2': 'Dir_of_Fastest_2Min_Wind',
                            'WDF5': 'Dir_of_Fastest_5Sec_Wind',
                            'WSF2': 'Fastest_2min_Wind_Speed',
                            'WSF5': 'Fastest_5sec_Wind_Speed'}, inplace=True)

#Fill Empty Data with N/A
weather_df = weather_df.fillna('N/A')
#Add an Index
weather_df['ID'] = weather_df.index
col_weather_id = weather_df.pop('ID')
weather_df.insert(0, 'ID', col_weather_id)

#Check Final Results
#print(weather_df.head())
#print(weather_df.info())
#print(weather_df.isna().sum())

#--- Part 3: Create the Databases ---#
#Create the Trip Info DB
Trip_Info = bike_df[['ID', 'Date', 'Start_Time', 'Stop_Time', 'Trip_Duration', 'Start_Station_ID', 'End_Station_ID', 'Bike_ID', 'User_Type', 'Gender', 'Birth_Year']]

#Create Station Info DB
Start_Stations = bike_df[['Start_Station_ID', 'Start_Station_Name', 'Start_Station_Latitude', 'Start_Station_Longitude']]
End_Stations = bike_df[['End_Station_ID', 'End_Station_Name', 'End_Station_Latitude', 'End_Station_Longitude']]
Station_Info = pd.concat([Start_Stations, End_Stations]).drop_duplicates()
Station_Info.rename(columns={'Start_Station_ID': 'ID',
                             'Start_Station_Name': 'Station_Name',
                             'Start_Station_Latitude': 'Station_Latitude',
                             'Start_Station_Longitude': 'Station_Longitude'}, inplace=True)
Station_Info = Station_Info.drop(['End_Station_ID', 'End_Station_Name', 'End_Station_Latitude', 'End_Station_Longitude'], axis=1)
Station_Info.reset_index(drop=True, inplace=True)
Station_Info = Station_Info.dropna()
Station_Info['ID'] = Station_Info['ID'].astype(int)

#Create User Info DB
User_Info = bike_df[['User_Type', 'Gender', 'Birth_Year']].drop_duplicates().reset_index(drop=True)
User_Info['User_ID'] = User_Info.index
User_Info_ID = User_Info.pop('User_ID')
User_Info.insert(0, 'User_ID', User_Info_ID)

#Add User_ID from User Info in Trip Info
Trip_Info = Trip_Info.merge(User_Info[['User_ID', 'User_Type', 'Gender', 'Birth_Year']],
                            on=['User_Type', 'Gender', 'Birth_Year'],
                            how='left')
Trip_Info = Trip_Info.drop(['User_Type', 'Gender', 'Birth_Year'], axis=1)

#Create Weather Info DB
Weather_Info = weather_df

#Check Databases
print(Trip_Info)
print(Station_Info)
print(User_Info.info())
print(Weather_Info.info())

#--- Part 4: Create SQL Databases ---#
#Create SQL Tables
Base = declarative_base()

class Trip_Information(Base):
    __tablename__ = 'Trip Information'

    ID = Column('ID', Integer, primary_key=True)
    Date = Column('Date', DATE, ForeignKey('Weather Information.DATE'))
    Start_Time = Column('Start_Time', TIME)
    Stop_Time = Column('Stop_Time', TIME)
    Trip_Duration = Column('Trip_Duration', Integer)
    Start_Station_ID = Column('Start_Station_ID', Integer, ForeignKey('Station Information.ID'))
    End_Station_ID = Column('End_Station_ID', Integer, ForeignKey('Station Information.ID'))
    Bike_ID = Column('Bike_ID', Integer)
    User_Id = Column('User_ID', Integer, ForeignKey('User Information.User_ID'))

class Station_Information(Base):
    __tablename__ = 'Station Information'

    ID = Column('ID', String, primary_key=True)
    Station_Name = Column('Station_Name', String)
    Station_Latitude = Column('Station_Latitude', String)
    Station_Longitude = Column('Station_Longitude', String)

class User_Information(Base):
    __tablename__ = 'User Information'

    User_ID = Column('User_ID', Integer, primary_key=True)
    User_Type = Column('User_Type', String)
    Gender = Column('Gender', String)
    Birth_Year = Column('Birth_Year', String)

class Weather_Information(Base):
    __tablename__ = 'Weather Information'

    ID = Column('ID', Integer, primary_key=True)
    DATE = Column('DATE', DATE)
    Avg_Wind_Speed = Column('Avg_Wind_Speed', Integer)
    Precipitation = Column('Precipitation', Integer)
    Snowfall = Column('Snowfall', Integer)
    Snow_Depth = Column('Snow_Depth', Integer)
    Avg_Temp = Column('Avg_Temp', Integer)
    Max_Temp = Column('Max_Temp', Integer)
    Min_Temperature = Column('Min_Temperature', Integer)
    Dir_of_Fastest_2Min_Wind = Column('Dir_of_Fastest_2Min_Wind', Integer)
    Dir_of_Fastest_5Sec_Wind = Column('Dir_of_Fastest_5Sec_Wind', Integer)
    Fastest_2min_Wind_Speed = Column('Fastest_2min_Wind_Speed', Integer)
    Fastest_5sec_Wind_Speed = Column('Fastest_5sec_Wind_Speed', Integer)

engine = create_engine('sqlite:///bike_rental.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

#Insert all Data
Trip_Info.to_sql('Trip Information', con=engine, if_exists='replace', index=False)
Station_Info.to_sql('Station Information', con=engine, if_exists='replace', index=False)
User_Info.to_sql('User Information', con=engine, if_exists='replace', index=False)
Weather_Info.to_sql('Weather Information', con=engine, if_exists='replace', index=False)

#--- Part 5: Create Views ---#
connection = sqlite3.connect('bike_rental.db')
curs = connection.cursor()
curs.execute('''CREATE VIEW Average_Trip_Duration AS
                SELECT Date, AVG(Trip_Duration) AS Avg_Trip_Duration
                FROM 'Trip Information'
                GROUP BY Date
                ORDER BY Date ASC''')

curs.execute('''CREATE VIEW Daily_Number_of_Trips AS
                SELECT Date, Count(Start_Time) AS Daily_Number_of_Trips
                FROM 'Trip Information'
                GROUP BY Date
                ORDER BY Date ASC''')

curs.execute('''CREATE VIEW Station_Count AS
                SELECT s.Station_ID,
                        s.Start_Station_Count,
                        e.End_Station_Count
                FROM (
                    SELECT Start_Station_ID AS Station_ID, 
                        COUNT(*) AS Start_Station_Count
                    FROM "Trip Information"
                    GROUP BY Start_Station_ID
                ) s
                LEFT JOIN (
                    SELECT End_Station_ID AS Station_ID, 
                        COUNT(*) AS End_Station_Count
                    FROM "Trip Information"
                    GROUP BY End_Station_ID
                ) e ON s.Station_ID = e.Station_ID
                ORDER BY s.Station_ID ASC''')

connection.commit()
connection.close()