import pandas as pd
import os

path = os.getcwd()
inputfile = os.path.join(path, 'data.csv')
outputfile = os.path.join(path, 'data.json')

dbname = None

def get_workspace():
    ''' return current working directory '''
    path = os.getcwd()
    print(f'Workspace: {path}')
    return path

def set_workspace(path):
    ''' sets home directory '''
    os.chdir(path)
    
##def show_databases():
##    ''' show existing dirs '''
##    dirs = []
##    for i in os.listdir():
##        dirs.append(i)
##        print(i)
##    return dirs
        
def create_database(name):
    ''' creates a data base with arg:name '''
    if name in os.listdir():
        print('Database already exists!')
    else:
        os.mkdir(name)

def use_database(name):
    ''' using database with arg:name '''
    path = os.path.join(path,name)
    os.chdir(path)

def read_data(inputfile, query="select", _all=True):
    ''' reads csv file and saves it as json file'''
    file = pd.read_csv(inputfile)
    
    try:
        if query=='select' and _all==True:
            print(file)
    except:
        print('query param is missing')

def show_databases():
    ''' show database (dirs) in current path '''
    dbnames = []
    dbnames.append(path.split('\\'))
    dbname = dbnames[0][-1]
    print(dbname)

def show_tables(ttype='csv'):
    ''' shows data from a file '''
    tables = [i for i in os.listdir(os.getcwd())]

    for i in tables:
        if ttype in i:
            print(i.split('.')[0])


def create_table(schema=[], name='table', print_schema=True):
    ''' creates table with cols c1, c2, c3 '''
    table = pd.DataFrame(columns=schema)
    table.name = name
    if print_schema==True:
        print(table)
    return table

def insert_row(table, values=[], print_table=True):
    ''' insert data row to table '''
    table.loc[len(table.index)] = values
    if print_table==True:
        print(table) 
    return table

def insert_column(table, col_name=None, col=[], print_table=True):
    ''' insert data column to table '''
    table[col_name] = col
    if print_table==True:
        print(table) 
    return table


def commit_table(table, json=False):
    ''' commits changes to table arg:table_name is pandas table '''
    file_name = os.path.join(path, table.name+'.csv')
    pout = os.path.join(path, file_name)
    
    if json==True:
        table.to_json(pout)
    else:
        table.to_csv(pout)


if __name__ == "__main__":

##    print(path)
##    part 1: administration/management
##    get_workspace()
##    show_databases()
##    pathch = os.path.join(get_workspace(), 'testdb') 
##    set_workspace(pathch)
##    get_workspace()
##    show_databases()
##    read_data(inputfile, query="select", _all=True)
    
##sql
    rows =[]
    cols=[]

    dict_data = []
    for i in range(5):
        rows.append(f'r{i}')
        cols.append(f'c{i}')
    
    table=create_table(cols)
    table = insert_row(table, values=[1, 2, 3, 4, 5], print_table=False)
    table = insert_row(table, values=[1, 2, 3, 4, 5], print_table=False)
    table = insert_row(table, values=[1, 2, 3, 4, 5], print_table=False)
    table = insert_row(table, values=[1, 2, 3, 4, 5], print_table=True)
    commit_table(table, json=False)

    show_tables(ttype='json')

    some_col = []
    for i in range(len(table)):
        some_col.append(i)
    insert_column(table, col_name='c5', col=some_col, print_table=True)
