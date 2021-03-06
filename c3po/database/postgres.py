import psycopg2 as ps
import pandas as pd
from martha import cleanUpString


def queryPostgres(host, port, user, password, database, query):
    '''
    Submit a blocking query to Postgres
    -----------
    DETAILS
    -----------
    This function will send a query to a Postgres compliant database (including Amazon Redshift and Amazon Aurora).
    The query will wait for a response with the data which means any queries will need to resolve within 5 minutes if
    invoking on as a Lambda Function (or 30 seconds if invoking the lambda through HTTP).
    -----------
    PARAMS
    -----------
    host : The hostname of the database to connec todo
    port : The port that accepts connections
    user : username that has permission to execute queries
    password : The password for authentication
    database : The database one the Postgres instance to run the query against
    query : The query to execute
    '''
    try:
        conn = ps.connect("dbname='" + database + "' user='" + user + "' host='" + host + "' port='" + port + "' password='" + password + "'")
        cur = conn.cursor()
        cur.execute(query)

        columns = []
        for i in range(len(cur.description)):
            columns.append(cur.description[i].name)
            pass

        rows = cur.fetchall()
        data = pd.DataFrame(rows, columns=[columns])
        conn.close
        return data
    except Exception as e:
        raise Exception(str(e))


def createFieldReplacement(repeats):
        repeats = repeats - 1
        fieldReplacement = "%s, "
        fieldReplacement = fieldReplacement * repeats
        fieldReplacement = fieldReplacement + "%s::json"
        fieldReplacement = "(" + fieldReplacement + ")"
        return fieldReplacement


def insertToPostgres(host, port, username, password, database, table, data, columns, upsertPrimaryKey=None):
    try:
        # data = data.where((pd.notnull(data)), None)
        rowsToInsert = len(data)
        fieldReplacement = createFieldReplacement(len(data.keys()))
        conn = ps.connect("dbname='" + database + "' user='" + username + "' host='" + host + "' port='" + port + "' password='" + password + "'")
        cur = conn.cursor()
        allRowSql = bytes(b"INSERT INTO " + table.encode() + b" (" + cleanUpString(str(data.columns.values.tolist()), ["[", "]", "'"], {"'": ""}).encode() + b") VALUES ")

        for i in range(rowsToInsert):
            row = data.iloc[i].values.tolist()
            if i == (rowsToInsert - 1):
                rowSql = cur.mogrify(fieldReplacement, (row))
            else:
                rowSql = cur.mogrify(fieldReplacement, (row)) + b","

            allRowSql = allRowSql + rowSql

        if upsertPrimaryKey is not None:
            upsertPrimaryKey = str(upsertPrimaryKey).replace("[", "").replace("]", "").replace("'", "")

            baseUpsert = b" ON CONFLICT (" + upsertPrimaryKey.encode() + b") DO UPDATE SET "

            allRowSql = allRowSql + baseUpsert

            for i in range(len(data.columns)):
                if i == (len(data.columns) - 1):
                    columnUpsert = data.columns.values.tolist()[i].encode() + b" = EXCLUDED." + data.columns.values.tolist()[i].encode()
                    allRowSql = allRowSql + columnUpsert
                else:
                    columnUpsert = data.columns.values.tolist()[i].encode() + b" = EXCLUDED." + data.columns.values.tolist()[i].encode() + b","
                    allRowSql = allRowSql + columnUpsert

        cur.execute(allRowSql)
        conn.commit()
        conn.close()
        results = {"columns": len(data.columns), "rows": len(data)}
        return results
    except Exception as e:
        conn.close()
        raise Exception(str(e))
