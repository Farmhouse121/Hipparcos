#!/usr/bin/env python3
"""Extract and insert Hipparcos data."""

from sys import stderr, stdout, version_info
from pymysql import connect, DatabaseError, OperationalError
from pymysql.cursors import DictCursor
from warnings import filterwarnings

class Break(Exception):
    """Permits deep breaks."""
    pass
    
def main():
    # modules
    from os import getenv,chmod

    # arguments
    from argparse import ArgumentParser;
    args = ArgumentParser();
    args.add_argument("-D","--database", type=str, default="database=Analysis", help='Database connection.')
    args.add_argument("-H","--hidden", action='store_true', help="Prevent arguments and secrets being echoed to the terminal.")
    args.add_argument("-U","--update", action='store_true', help='Update database data.')
    args = args.parse_args();

    # initialize
    print("%s %s\nParameters: %s" % (__doc__,str(datetime.now())[:19],vars(args) if not args.hidden else 'hidden'))

    database = dict(map(lambda x:x.split('='), args.database.split(';'))) # converts "a=b;c=d" into {a:b,c:d}

    if 'pwd' not in database or database['pwd'] == '' or database['pwd'] == None:
        database['pwd'] = getenv('MYSQLPASSWORD')

    if database['pwd'] == None or database['pwd'] == '':
        from getpass import getpass
        database['pwd'] = getpass('Database password:')

    if 'server' not in database or database['server'] == '' or database['server'] == None:
        database['server'] = 'localhost'

    # connect to database
    print("Connecting to database %s." % (args.database if not args.hidden else 'hidden'))

    try:
        connection = connect(
            db=database['database'] if 'database' in database else 'mysql',
            host=database['server'] if 'server' in database else 'localhost',
            port=int(database['port']) if 'port' in database else 3306,
            user=database['uid'] if 'uid' in database else getenv('USER'),
            password=database['pwd'],
            cursorclass=DictCursor,
            autocommit=True
        )

    except OperationalError as x:
        stdout.flush()
        stderr.write("%s\n" % str(x))
        exit(1) # raise an exception for a more severe outcome

    if args.update:
        filterwarnings('ignore', category=Warning)
    
    sql,requests = None,[]
    
    try:
        with connection.cursor() as cursor:
            pass # this is just a shell, put code here
                
    except DatabaseError:
        if "sql" in locals() and sql!=None:
            stdout.flush()
            stderr.write("Problem with SQL:\n%s\n" % sql)

        raise

    except KeyboardInterrupt:
        stdout.flush()
        stderr.write("Interrupted!\n")

    except Break:
        stdout.flush()
        stderr.write("Deep break!\n")

    # done
    print("Done.")

# bootstrap
if __name__ == "__main__":
    assert(version_info.major >= 3)
    main()
