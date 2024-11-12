from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, Float, Date, Integer, ForeignKey, MetaData
from sqlalchemy.exc import SQLAlchemyError

engine = create_engine('sqlite:///database_alchemy.db')
files = ['clean_measure.csv', 'clean_stations.csv']

def creating_connection(engine):
    '''
    Create connection with database SQLAlchemy
    :param engine: Name of database
    '''
    def decorator(func):
        '''
        Decorating selected function
        Catch error with connection
        :param *args, **kwargs: params of functions
        :param conn: Connection with database
        '''
        def wrapper(*args, **kwargs):
            with engine.connect() as conn:
                result = func(conn, *args, **kwargs)
            return result
        return wrapper
    return decorator

@creating_connection(engine)
def createTable(conn):
    '''
    Create tables based on files in list files
    :param conn: Connection with database
    '''
    meta = MetaData()

    stations = Table(
            'stations', meta,
            Column('station', String, primary_key = True),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('elevation', Float),
            Column('name',String),
            Column('country', String),
            Column('state', String),
    )

    measures = Table(
        'measures', meta,
        Column('station', String, ForeignKey('stations.station'),primary_key = True),
        Column('date', Date),
        Column('precip', Float),
        Column('tobs', Integer),
    )

    meta.create_all(engine)

createTable()