#!/usr/bin/env python3
"""Extract and insert Hipparcos data."""

from sys import stderr, stdout, version_info
from pymysql import connect, DatabaseError, OperationalError, InternalError
from pymysql.cursors import DictCursor
from warnings import filterwarnings

class Break(Exception):
    """Permits deep breaks."""
    pass
    
def main():
    # modules
    from os import getenv,chmod
    from re import match,sub
    from tempfile import NamedTemporaryFile

    # arguments
    from argparse import ArgumentParser;
    args = ArgumentParser(epilog="""This script will upload the raw text version of the Hipparcos catalogue into a MySQL database.
You supply the name of a folder that contains both the ReadMe file for the catalogue and the raw data file. It will read the catalogue
schema from the ReadMe file, create a table in the MySQL database based upon that schema, and insert the data. The insert is done
via LOAD DATA INFILE for speed and the data file is copied to /tmp/ to permit this. If you supply --droptable then the SQL command
DROP TABLE IF EXISTS <tablename> will be issued. Table creation is done via CREATE TABLE IF NOT EXISTS <tablename>, so --droptable
is needed to alter the schema of the database copy of the table. The load is done via REPLACE INTO TABLE <tablename>, so existing
data is replaced by new data always.""");
    args.add_argument("-D","--database", type=str, default="database=Analysis", help='Database connection.')
    args.add_argument("-H","--hidden", action='store_true', help="Prevent arguments and secrets being echoed to the terminal.")
    args.add_argument("-U","--update", action='store_true', help='Update database data.')
    args.add_argument("folder",type=str,help="Folder name for Hipparcos data.")
    args.add_argument("-R","--readme",type=str,default='ReadMe',help="Name of ReadMe file.")
    args.add_argument("-F","--datafile",type=str,default=None,help="Name of Hipparcos data file (defaults to standard based on catalogue).")
    args.add_argument("-T","--tablename",type=str,default=None,help="Name of Hipparcos data table (defaults to catalogue name).")
    args.add_argument("-C","--catalogue",type=str,default='Hipparcos',choices=('Hipparcos','Tycho'),help="Catalogue to read.")
    args.add_argument("-X","--droptable",action='store_true')
    args.add_argument("-E","--expect",type=int,default=None,help="Number of fields to expect.")
    args.add_argument("-K","--key",type=str,default=None,help="Unique key specification.")
    args.add_argument("-B","--heartbeat",type=int,default=1000,help="Status update frequency.")
    args.add_argument("-G","--geometry",action='store_true',help="Set to add geometry fields for spatial extensions.")
    args = args.parse_args();
    
    if args.tablename==None:
        args.tablename=args.catalogue
        
    elif "%s" in args.tablename:
        args.tablename=args.tablename % args.catalogue
        
    if args.datafile==None:
        args.datafile='hip_main.dat' if args.catalogue=='Hipparcos' else 'tyc_main.dat'

    if args.expect==None:
        args.expect=76 if args.catalogue=='Hipparcos' else 67
        
    if args.key==None:
        args.key='unique key unique_key (`%s`)' % ('HIP' if args.catalogue=='Hipparcos' else 'TYC')

    # initialize
    print("%s\nParameters: %s" % (__doc__,vars(args) if not args.hidden else 'hidden'))

    database = dict(map(lambda x:x.split('='), args.database.split(';'))) # converts "a=b;c=d" into {a:b,c:d}

    if 'pwd' not in database or database['pwd'] == '' or database['pwd'] == None:
        database['pwd'] = getenv('MYSQLPASSWORD')

    if database['pwd'] == None or database['pwd'] == '':
        from getpass import getpass
        database['pwd'] = getpass('Database password:')

    if 'server' not in database or database['server'] == '' or database['server'] == None:
        database['server'] = 'localhost'

    # initial processing on files
    filename=args.folder+'/'+args.readme
    print("Opening %s to read table schema." % filename)
    fields=[]
    
    with open(filename) as datafile:
        # find schema for the file
        try:
            for line in datafile:
                if "Byte-by-byte" in line and args.datafile in line:
                    print("%s schema:" % args.catalogue)
                    
                    for i in range(3):
                        datafile.readline()
                    
                    for line in datafile:
                        if '-----------' in line:
                            raise Break() # deep break
                        
                        if (tokens:=match(r"(\s*\d+-?\s*\d+) +([A-Z][0-9.]+) +([^\s]+) +([^\s]+) +(.+)",line)):
                            crange,ctype,cunits,cname,comment=tokens.groups()
                            
                            if cname=='---':
                                continue # ignore the repeated record for the HIP number
                                
                            cname=sub(r"[-:()]","",cname)
                            
                            if '-' in crange:
                                crange=list(map(int,crange.split('-')))
                                
                            else:
                                crange=[int(crange)]*2
                                
                            crange+=[crange[1]-crange[0]+1]
                                
                            comment=sub(r"\s+"," ",sub(r"^\*?(\[.+\])?\??\s*","",comment))
                            
                            if ctype[0]=='A':
                                ctype='varchar(%s)' % ctype[1:]
                                
                            elif ctype[0]=='I':
                                ctype='int'
                                
                            elif ctype[0]=='F':
                                ctype='decimal(%s,%s)' % tuple(ctype[1:].split('.'))
                                
                            column="`%s` %s COMMENT '%s%s'" % (cname,ctype,comment.replace("'","''"),", "+cunits if cunits!="---" else "")
                                
                            fields.append({
                                'column':column,
                                'source':line.strip(),
                                'range':crange,
                                'type':ctype,
                                'units':(cunits if cunits!='---' else None),
                                'name':cname,
                                'comment':comment
                            })
                            
                            stdout.write(line)
        except Break:
            pass
    
    if len(fields)!=args.expect:
        raise ValueError("Incorrect field count of %d when expecting %d in file %s." % (len(fields),args.expect,filename))
    
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
    
    sql=None
    
    try:
        with connection.cursor() as cursor:
            # drop table
            sql="DROP TABLE IF EXISTS `%s`" % args.tablename
            
            if args.update:
                cursor.execute(sql)
                print("WARNING: Dropped table `%s`.")
                
            else:
                print(sql)
                
            # create table
            sql="""/* CREATING HIPPARCOS DATA TABLE */
CREATE TABLE IF NOT EXISTS `%s` (
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    %s,
    %s
);""" % (args.tablename,",\n    ".join(map(lambda x:x['column'],fields)),args.key)

            if args.update:
                cursor.execute(sql)
                print("Created table `%s` if needed." % args.tablename)
                
            else:
                print(sql)
            
            # bulk insert the data
            filename=args.folder+'/'+args.datafile
            print("Copying %s to bulk insert area." % filename)

            with NamedTemporaryFile(dir='/tmp') as loadfile,open(filename) as datafile:
                n=0
            
                for line in datafile:
                    loadfile.write(line.encode('utf8','ignore')) # unicode sucks
                    n+=1
                    
                    if (n%args.heartbeat)==0:
                        stdout.write("."+("\n" if n/args.heartbeat==80 else ""))
                        stdout.flush()
                
                loadfile.flush()
                chmod(loadfile.name,0o644) # so the SQL server can read the file
                
                # execute bulk insert
                sql="""LOAD DATA INFILE '%s' REPLACE INTO TABLE `%s` (@Record) SET %s""" % (
                    loadfile.name,
                    args.tablename,
                    ",\n    ".join(map(lambda field:"`%s`=CASE WHEN SUBSTR(@Record,%d,%d)<>REPEAT(' ',%d) THEN SUBSTR(@Record,%d,%d) END" % (
                        field['name'],
                        field['range'][0],
                        field['range'][2],
                        field['range'][2],
                        field['range'][0],
                        field['range'][2]
                    ),fields))
                )

                if args.update:
                    print("\nBeginning bulk insert from %s." % loadfile.name)
                    cursor.execute(sql)
                    print("Stored %d records." % n)
                    
                else:
                    print("\n",sql)
                
    except (DatabaseError,InternalError):
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
