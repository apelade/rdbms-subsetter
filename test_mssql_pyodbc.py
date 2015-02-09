import os
import sys
import unittest
import tempfile
import sqlite3
from sqlalchemy import create_engine
from subsetter import Db

class DummyArgs(object):
    logarithmic = False
    fraction = 0.25
    force_rows = {}
    children = 25

dummy_args = DummyArgs()


UP = [
         """CREATE TABLE state(
         abbrev nchar(2) NOT NULL,
         name nvarchar(50) NOT NULL,
         CONSTRAINT PK_state PRIMARY KEY (abbrev)
         )""",

         """CREATE TABLE city (
         name nvarchar(50),
         state_abbrev nchar(2),
         CONSTRAINT PK_city PRIMARY KEY (name),
         FOREIGN KEY (state_abbrev) 
         REFERENCES state(abbrev)
         )""",

         """CREATE TABLE landmark (
         name nvarchar(50),
         city nvarchar(50),
         FOREIGN KEY (city)
         REFERENCES city(name)
         )""",

         """CREATE TABLE zeppelins (
         name nvarchar(50),
         home_city nvarchar(50),
         FOREIGN KEY (home_city)
         REFERENCES city(name)
         )""", # NULL FKs

         """CREATE TABLE languages (name nvarchar(50))""", # empty table
     ]
     
DOWN = [
         " IF EXISTS (SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name='languages') DROP TABLE languages",
         " IF EXISTS (SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name='zeppelins') DROP TABLE zeppelins",
         " IF EXISTS (SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name='landmark') DROP TABLE landmark",
         " IF EXISTS (SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name='city') DROP TABLE city",
         " IF EXISTS (SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name='state') DROP TABLE state",
       ]
      
class OverallTest(unittest.TestCase):

    source_sqla = 'mssql+pyodbc://sa:password1@dev_subsetter_proj'
    dest_sqla = 'mssql+pyodbc://sa:password1@dev_subsetter_tiny'

    source_db = create_engine(source_sqla).connect()
    dest_db = create_engine(dest_sqla).connect()
    
    def setUp(self):                
        for statement in DOWN + UP:
            self.source_db.execute(statement)
            self.dest_db.execute(statement)
        
#        self.source_db.execute("SET NOCOUNT ON")
#        self.source_db.execute("SET IDENTITY_INSERT %s ON" tbl.name)
#        self.source_db.execute("SET IDENTITY_INSERT %s OFF" tbl.name)
#        self.source_db.execute("SET NOCOUNT OFF")

        for params in (('MN', 'Minnesota'), ('OH', 'Ohio'), ('MA', 'Massachussetts'), ('MI', 'Michigan')):
            self.source_db.execute("INSERT INTO state VALUES (?, ?)", params)
            
        for params in (('Duluth', 'MN'), ('Dayton', 'OH'), ('Boston', 'MA'), ('Houghton', 'MI')):
            self.source_db.execute("INSERT INTO city VALUES (?, ?)", params)
            
        for params in (('Lift Bridge', 'Duluth'), ("Mendelson's", 'Dayton'), ('Trinity Church', 'Boston'), ('Michigan Tech', 'Houghton')):
            self.source_db.execute("INSERT INTO landmark VALUES (?, ?)", params)
            
        for params in (('Graf Zeppelin', None), ('USS Los Angeles', None), ('Nordstern', None), ('Bodensee', None)):
            self.source_db.execute("INSERT INTO zeppelins VALUES (?, ?)", params)
            
#        self.source_db.commit()
#        self.dest_db.commit()
    

    
    def tearDown(self):
        for statement in DOWN:
           self.source_db.execute(statement)
           self.dest_db.execute(statement)        
        #languages_better_than_python.drop(self.dest_db, checkfirst=True)
        #zeppelins.drop(self.dest_db, checkfirst=True)
        #city.drop(self.dest_db, checkfirst=True)
        #state.drop(self.dest_db, checkfirst=True)
        self.source_db.close()
        #os.unlink(self.source_db_filename)
        self.dest_db.close()
        #os.unlink(self.dest_db_filename)
        

    def test_parents_kept(self):
        print("Parent test0") 
        src = Db(self.source_sqla, dummy_args)

        dest = Db(self.dest_sqla, dummy_args)
        print("Parent test1") 

        src.assign_target(dest)
        print("Parent test2") 

        src.create_subset_in(dest)
        print("Parent test3") 

        cities = self.dest_db.execute("SELECT * FROM city").fetchall()
        self.assertEqual(len(cities), 1)
        joined = self.dest_db.execute("""SELECT c.name, s.name
                                         FROM city c JOIN state s 
                                                     ON (c.state_abbrev = s.abbrev)""")
        joined = joined.fetchall()
        self.assertEqual(len(joined), 1)
             
    def test_null_foreign_keys(self):
        src = Db(self.source_sqla, dummy_args)
        dest = Db(self.dest_sqla, dummy_args)
        src.assign_target(dest)
        src.create_subset_in(dest)
        zeppelins = self.dest_db.execute("SELECT * FROM zeppelins").fetchall()
        self.assertEqual(len(zeppelins), 1)


#suite = unittest.TestLoader().loadTestsFromTestCase(OverallTest)
#unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    unittest.main()